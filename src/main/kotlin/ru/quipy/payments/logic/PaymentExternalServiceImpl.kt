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
import java.io.IOException
import java.io.InterruptedIOException

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metrics: PaymentMetrics,
    private val providerCallTimeoutMillis: Long,
) : PaymentExternalSystemAdapter {
    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
        private val emptyBody = ByteArray(0).toRequestBody(null)
    }

    private val admission = AccountAdmission(properties.rateLimitPerSec, properties.parallelRequests)
    private val responseReserve = properties.averageProcessingTime.plusMillis(250)
    private val client = OkHttpClient.Builder().retryOnConnectionFailure(false)
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .followRedirects(false).followSslRedirects(false).build()

    init {
        require(providerCallTimeoutMillis > 0) { "Provider call timeout must be positive" }
        metrics.registerAccount(properties.accountName)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        var admitted = false
        var submitted = false
        var success = false
        var attempted = false
        var outcome = PaymentMetrics.Outcome.DEADLINE
        var reason: String? = "Deadline leaves insufficient time for payment"
        try {
            admitted = admission.acquireSlot(deadline, responseReserve)
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
                if (admission.acquireRatePermit(deadline, responseReserve)
                    && deadline - now() > responseReserve.toMillis()) {
                    call.timeout().timeout(providerCallTimeoutMillis, TimeUnit.MILLISECONDS)
                    metrics.providerStarted(properties.accountName)
                    attempted = true
                    call.execute().use { response ->
                        if (response.isSuccessful) {
                            val body = mapper.readValue(response.body!!.string(), ExternalSysResponse::class.java)
                            success = body.result
                            reason = body.message
                            outcome = when {
                                success -> PaymentMetrics.Outcome.SUCCESS
                                body.message == "parallel_requests_limit_breached" -> PaymentMetrics.Outcome.PARALLEL_LIMIT
                                body.message == "rate_limit_breached" -> PaymentMetrics.Outcome.RATE_LIMIT
                                else -> PaymentMetrics.Outcome.PROVIDER_REJECTED
                            }
                        } else {
                            reason = "Provider HTTP ${response.code}"
                            outcome = PaymentMetrics.Outcome.HTTP_ERROR
                        }
                    }
                }
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            reason = "Payment interrupted"
            outcome = PaymentMetrics.Outcome.INTERRUPTED
        } catch (e: Exception) {
            reason = "Payment error: ${e.javaClass.simpleName}"
            outcome = when (e) {
                is InterruptedIOException -> PaymentMetrics.Outcome.TIMEOUT
                is IOException -> PaymentMetrics.Outcome.IO_ERROR
                else -> PaymentMetrics.Outcome.INTERNAL_ERROR
            }
        } finally {
            if (admitted) admission.release()
            if (attempted) metrics.providerFinished(properties.accountName, outcome)
        }
        if (!submitted) {
            paymentESService.update(paymentId) {
                it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }
        if (success && now() >= deadline) {
            outcome = PaymentMetrics.Outcome.LATE_SUCCESS
        }
        paymentESService.update(paymentId) {
            it.logProcessing(success, now(), transactionId, reason)
        }
        metrics.completed(outcome)
        logger.info("Payment {} account {} outcome {} attempted {}", paymentId, properties.accountName, outcome, attempted)
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
