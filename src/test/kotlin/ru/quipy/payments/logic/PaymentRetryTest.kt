package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.RETURNS_DEFAULTS
import org.mockito.stubbing.Answer
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class PaymentRetryTest {
    @Test
    fun `temporary refusal is retried with one final payment result`() {
        Provider { exchange, attempt -> respond(exchange, attempt > 1) }.use { provider ->
            val payment = Payment(provider)
            payment.run()

            assertEquals(2, provider.requests.size)
            assertEquals(1, provider.requests.map { it["paymentId"] }.distinct().size)
            assertEquals(2, provider.requests.map { it["transactionId"] }.distinct().size)
            assertEquals(2, payment.events.filterIsInstance<PaymentSubmittedEvent>().size)
            assertTrue(payment.events.filterIsInstance<PaymentProcessedEvent>().single().success)
            assertEquals(1.0, payment.counter("online.shop.payment.retries"))
            assertEquals(1.0, payment.counter("online.shop.payment.completed", "SUCCESS"))
            assertEquals(1.0, payment.counter("online.shop.provider.finished", "TEMPORARY_ERROR"))
        }
    }

    @Test
    fun `persistent temporary errors stop after three attempts`() {
        Provider { exchange, _ -> respond(exchange, false) }.use { provider ->
            val payment = Payment(provider)
            payment.run()

            assertEquals(3, provider.requests.size)
            assertEquals(2.0, payment.counter("online.shop.payment.retries"))
            assertFalse(payment.events.filterIsInstance<PaymentProcessedEvent>().single().success)
            assertEquals(1.0, payment.counter("online.shop.payment.completed", "TEMPORARY_ERROR"))
        }
    }

    @Test
    fun `success permanent refusal and HTTP errors are not retried`() {
        for ((success, message, status) in listOf(
            Triple(true, "", 200), Triple(false, "Declined", 200), Triple(false, "Temporary error", 503)
        )) {
            Provider { exchange, _ -> respond(exchange, success, message, status) }.use { provider ->
                val payment = Payment(provider)
                payment.run()

                assertEquals(1, provider.requests.size)
                assertEquals(0.0, payment.counter("online.shop.payment.retries"))
                assertEquals(success && status == 200, payment.events.filterIsInstance<PaymentProcessedEvent>().single().success)
            }
        }
    }

    @Test
    fun `uncertain timeout does not cause a second charge`() {
        Provider { exchange, _ ->
            Thread.sleep(500)
            respond(exchange, true)
        }.use { provider ->
            val payment = Payment(provider, timeoutMillis = 150)
            payment.run()

            assertEquals(1, provider.requests.size)
            assertEquals(0.0, payment.counter("online.shop.payment.retries"))
            assertEquals(1.0, payment.counter("online.shop.payment.completed", "TIMEOUT"))
        }
    }

    @Test
    fun `another attempt is skipped when the deadline has no response budget`() {
        val deadline = AtomicLong()
        Provider { exchange, _ ->
            TimeUnit.MILLISECONDS.sleep(maxOf(0, deadline.get() - now() - 310))
            respond(exchange, false)
        }.use { provider ->
            val payment = Payment(provider, timeoutMillis = 2000)
            deadline.set(now() + 1500)
            payment.run(deadline.get())

            assertEquals(1, provider.requests.size)
            assertEquals(0.0, payment.counter("online.shop.payment.retries"))
            assertFalse(payment.events.filterIsInstance<PaymentProcessedEvent>().single().success)
        }
    }

    @Test
    fun `retry uses the same provider rate limiter and releases the parallel slot`() {
        Provider { exchange, attempt -> respond(exchange, attempt > 1) }.use { provider ->
            val payment = Payment(provider, rate = 1)
            payment.run()

            assertEquals(2, provider.requests.size)
            assertTrue(provider.sentAt[1] - provider.sentAt[0] >= TimeUnit.MILLISECONDS.toNanos(990))
            assertTrue(payment.events.filterIsInstance<PaymentProcessedEvent>().single().success)
        }
    }

    private class Provider(private val handle: (HttpExchange, Int) -> Unit) : AutoCloseable {
        val requests = CopyOnWriteArrayList<Map<String, String>>()
        val sentAt = CopyOnWriteArrayList<Long>()
        private val executor = Executors.newCachedThreadPool()
        private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            executor = this@Provider.executor
            createContext("/external/process") { exchange ->
                exchange.requestBody.close()
                val parameters = exchange.requestURI.rawQuery.split('&').associate {
                    val (key, value) = it.split('=', limit = 2)
                    key to URLDecoder.decode(value, StandardCharsets.UTF_8)
                }
                requests.add(parameters)
                sentAt.add(System.nanoTime())
                try {
                    handle(exchange, requests.size)
                } finally {
                    exchange.close()
                }
            }
            start()
        }
        val hostPort get() = "127.0.0.1:${server.address.port}"

        override fun close() {
            server.stop(0)
            executor.shutdownNow()
        }
    }

    private class Payment(provider: Provider, rate: Int = 10, timeoutMillis: Long = 2000) {
        private val paymentId = UUID.randomUUID()
        private val state = PaymentAggregateState().apply {
            paymentCreatedApply(create(paymentId, UUID.randomUUID(), 400))
        }
        val events = mutableListOf<Event<PaymentAggregate>>()
        private val registry = SimpleMeterRegistry()

        @Suppress("UNCHECKED_CAST")
        private val service = mock(EventSourcingService::class.java, Answer { invocation ->
            if (invocation.method.name == "update") {
                val command = invocation.arguments.last() as (PaymentAggregateState) -> Event<PaymentAggregate>
                val event = command(state)
                events.add(event)
                when (event) {
                    is PaymentSubmittedEvent -> state.paymentSubmittedApply(event)
                    is PaymentProcessedEvent -> state.paymentSubmittedApply(event)
                }
                null
            } else RETURNS_DEFAULTS.answer(invocation)
        }) as EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

        private val adapter = PaymentExternalSystemAdapterImpl(
            PaymentAccountProperties("test-shop", "acc-8", 1, rate, 30, Duration.ofMillis(10), true),
            service, provider.hostPort, "test-token", PaymentMetrics(registry, "test-shop"), timeoutMillis
        )

        fun run(deadline: Long = now() + 5000) = adapter.performPaymentAsync(paymentId, 400, now(), deadline)

        fun counter(name: String, outcome: String? = null): Double {
            val search = registry.find(name)
            if (outcome != null) search.tag("outcome", outcome)
            return search.counter()?.count() ?: 0.0
        }
    }

    companion object {
        private fun respond(exchange: HttpExchange, success: Boolean, message: String = "Temporary error", status: Int = 200) {
            val body = """{"transactionId":"test","paymentId":"test","result":$success,"message":"$message"}""".toByteArray()
            exchange.sendResponseHeaders(status, body.size.toLong())
            exchange.responseBody.use { it.write(body) }
        }
    }
}
