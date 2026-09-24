package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {
    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
        private val emptyBody = ByteArray(0).toRequestBody(null)
    }

    private val admission = AccountAdmission(properties.rateLimitPerSec, properties.parallelRequests)
    private val responseReserve = properties.averageProcessingTime.plusMillis(250)
    private val client = OkHttpClient.Builder().retryOnConnectionFailure(false)
        .followRedirects(false).followSslRedirects(false).build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        var admitted = false
        var submitted = false
        var success = false
        var reason: String? = "Deadline leaves insufficient time for payment"
        try {
            admitted = admission.acquire(deadline, responseReserve)
            if (admitted && deadline - now() > responseReserve.toMillis()) {
                val url = "http://$paymentProviderHostPort/external/process".toHttpUrl().newBuilder()
                    .addQueryParameter("serviceName", properties.serviceName)
                    .addQueryParameter("token", token)
                    .addQueryParameter("accountName", properties.accountName)
                    .addQueryParameter("transactionId", transactionId.toString())
                    .addQueryParameter("paymentId", paymentId.toString())
                    .addQueryParameter("amount", amount.toString()).build()
                val call = client.newCall(Request.Builder().url(url).post(emptyBody).build())
                paymentESService.update(paymentId) {
                    it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
                submitted = true
                val remaining = deadline - now()
                if (remaining > 0) {
                    call.timeout().timeout(remaining, TimeUnit.MILLISECONDS)
                    call.execute().use { response ->
                        if (response.isSuccessful) {
                            val body = mapper.readValue(response.body!!.string(), ExternalSysResponse::class.java)
                            success = body.result
                            reason = body.message
                        } else {
                            reason = "Provider HTTP ${response.code}"
                        }
                    }
                }
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            reason = "Payment interrupted"
        } catch (e: Exception) {
            reason = "Payment error: ${e.javaClass.simpleName}"
        } finally {
            if (admitted) admission.release()
        }
        if (!submitted) {
            paymentESService.update(paymentId) {
                it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }
        paymentESService.update(paymentId) {
            it.logProcessing(success, now(), transactionId, reason)
        }
        logger.info("Payment {} account {} success {}", paymentId, properties.accountName, success)
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
