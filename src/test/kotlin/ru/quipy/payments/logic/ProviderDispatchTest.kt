package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import okhttp3.OkHttpClient
import okhttp3.Request
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ProviderDispatchTest {
    @Test
    fun `dispatch lock is released after sending without waiting for the provider response`() {
        val received = CountDownLatch(3)
        val serializedResponses = AtomicBoolean(false)
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val handlers = Executors.newFixedThreadPool(3)
        val workers = Executors.newFixedThreadPool(3)
        server.executor = handlers
        server.createContext("/external/process") { exchange ->
            exchange.requestBody.close()
            received.countDown()
            if (!received.await(2, TimeUnit.SECONDS)) serializedResponses.set(true)
            val body = """{"transactionId":"test","paymentId":"test","result":true}""".toByteArray()
            exchange.sendResponseHeaders(200, body.size.toLong())
            exchange.responseBody.use { it.write(body) }
        }
        server.start()
        try {
            val admission = AccountAdmission(11, 64)
            val client = OkHttpClient.Builder().retryOnConnectionFailure(false).callTimeout(5, TimeUnit.SECONDS)
                .addNetworkInterceptor(ProviderRequestInterceptor(admission, Duration.ZERO)).build()
            val start = now()
            val requests = List(3) {
                workers.submit<Boolean> {
                    val request = Request.Builder().url("http://127.0.0.1:${server.address.port}/external/process")
                        .tag(ProviderDeadline::class.java, ProviderDeadline(start + 10_000))
                        .post(ProviderRequestBody(admission)).build()
                    client.newCall(request).execute().use { it.isSuccessful }
                }
            }
            requests.forEach { assertTrue(it.get(5, TimeUnit.SECONDS)) }
            assertEquals(0L, received.count)
            assertFalse(serializedResponses.get())
        } finally {
            server.stop(0)
            handlers.shutdownNow()
            workers.shutdownNow()
        }
    }
}
