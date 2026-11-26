package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.MediaType
import okhttp3.RequestBody
import okhttp3.Interceptor
import okhttp3.Response
import okio.BufferedSink
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
        private const val MAX_ATTEMPTS = 3
        private const val RETRY_DELAY_MILLIS = 100L
    }

    private val admission = AccountAdmission(properties.rateLimitPerSec, properties.parallelRequests)
    private val responseReserve = properties.averageProcessingTime.plusMillis(250)
    private val client = OkHttpClient.Builder().retryOnConnectionFailure(false)
        .addNetworkInterceptor(ProviderRequestInterceptor(admission, responseReserve))
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .followRedirects(false).followSslRedirects(false).build()

    init {
        require(providerCallTimeoutMillis > 0) { "Provider call timeout must be positive" }
        metrics.registerAccount(properties.accountName)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var attempt = 1
        var result = performAttempt(paymentId, amount, paymentStartedAt, deadline)
        while (result.outcome == PaymentMetrics.Outcome.TEMPORARY_ERROR && attempt < MAX_ATTEMPTS &&
            deadline - now() > responseReserve.toMillis() + RETRY_DELAY_MILLIS) {
            try {
                Thread.sleep(RETRY_DELAY_MILLIS)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                result = result.copy(outcome = PaymentMetrics.Outcome.INTERRUPTED, reason = "Payment interrupted")
                break
            }
            metrics.retried(properties.accountName)
            attempt++
            result = performAttempt(paymentId, amount, paymentStartedAt, deadline)
        }
        val outcome = if (result.success && now() >= deadline) PaymentMetrics.Outcome.LATE_SUCCESS else result.outcome
        paymentESService.update(paymentId) {
            it.logProcessing(result.success, now(), result.transactionId, result.reason)
        }
        metrics.completed(outcome)
        logger.info("Payment {} account {} outcome {} attempts {}", paymentId, properties.accountName, outcome, attempt)
    }

    private fun performAttempt(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): PaymentAttempt {
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
                val call = client.newCall(Request.Builder().url(url)
                    .tag(ProviderDeadline::class.java, ProviderDeadline(deadline))
                    .post(ProviderRequestBody(admission) {
                        metrics.providerStarted(properties.accountName)
                        attempted = true
                    }).build())
                paymentESService.update(paymentId) {
                    it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
                submitted = true
                if (deadline - now() > responseReserve.toMillis()) {
                    call.timeout().timeout(providerCallTimeoutMillis, TimeUnit.MILLISECONDS)
                    call.execute().use { response ->
                        if (response.isSuccessful) {
                            val body = mapper.readValue(response.body!!.string(), ExternalSysResponse::class.java)
                            success = body.result
                            reason = body.message
                            outcome = when {
                                success -> PaymentMetrics.Outcome.SUCCESS
                                body.message == "Temporary error" -> PaymentMetrics.Outcome.TEMPORARY_ERROR
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
                is ProviderDeadlineExceeded -> PaymentMetrics.Outcome.DEADLINE
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
        return PaymentAttempt(transactionId, success, outcome, reason)
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private data class PaymentAttempt(
        val transactionId: UUID,
        val success: Boolean,
        val outcome: PaymentMetrics.Outcome,
        val reason: String?,
    )
}

internal data class ProviderDeadline(val value: Long)

internal class ProviderDeadlineExceeded : IOException("Payment deadline leaves insufficient time for submission")

internal class ProviderRequestInterceptor(
    private val admission: AccountAdmission,
    private val responseReserve: Duration,
) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        val deadline = requireNotNull(chain.request().tag(ProviderDeadline::class.java)).value
        if (!admission.acquireRatePermit(deadline, responseReserve)) throw ProviderDeadlineExceeded()
        try {
            return chain.proceed(chain.request())
        } finally {
            admission.releaseRatePermit()
        }
    }
}

internal class ProviderRequestBody(
    private val admission: AccountAdmission,
    private val onSend: () -> Unit = {},
) : RequestBody() {
    override fun contentType(): MediaType? = null
    override fun contentLength() = 0L
    override fun writeTo(sink: BufferedSink) {
        onSend()
        sink.flush()
        admission.releaseRatePermit()
    }
}

fun now() = System.currentTimeMillis()
