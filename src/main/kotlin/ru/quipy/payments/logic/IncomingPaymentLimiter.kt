package ru.quipy.payments.logic

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import ru.quipy.common.utils.LeakyBucketMeterRateLimiter
import ru.quipy.common.utils.LeakyBucketQueueRateLimiter
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration

@Component
class IncomingPaymentLimiter(
    private val account: PaymentAccountProperties,
    private val metrics: PaymentMetrics,
    @param:Value("\${payment.ingress.algorithm:deadline-aware}") private val algorithm: String,
    @param:Value("\${payment.ingress.burst-capacity:11}") burstCapacity: Int,
    @param:Value("\${payment.ingress.retry-after-seconds:1}") val retryAfterSeconds: Long,
    @param:Value("\${payment.executor.workers:128}") workers: Int,
    @param:Value("\${payment.executor.queue-capacity:64}") queueCapacity: Int,
) {
    init {
        require(retryAfterSeconds > 0) { "Retry-After must be positive" }
        require(workers > 0 && queueCapacity > 0)
    }

    private val maxBufferedRequests = workers + queueCapacity
    private val incomingQueue = LeakyBucketQueueRateLimiter(account.rateLimitPerSec)
    private val limiter: RateLimiter = when (algorithm) {
        "deadline-aware" -> LeakyBucketMeterRateLimiter(account.rateLimitPerSec, maxBufferedRequests, paymentRateWindow)
        "sliding-window" -> SlidingWindowRateLimiter(account.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
        "leaky-bucket" -> LeakyBucketMeterRateLimiter(account.rateLimitPerSec, burstCapacity)
        else -> error("Unknown incoming limiter algorithm: $algorithm")
    }

    fun tryAcquire(deadline: Long, createdAt: Long = now()): Boolean {
        val accepted = if (algorithm == "deadline-aware") {
            val budgetMillis = bufferMillis(deadline, now())
            val needsQueue = bufferMillis(deadline, createdAt) * account.rateLimitPerSec / 1000L > maxBufferedRequests
            val ready = try {
                !needsQueue || incomingQueue.tryAcquire(Duration.ofMillis(budgetMillis))
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
            val hasTime = !needsQueue || deadline - now() > account.averageProcessingTime.toMillis() + 250L
            ready && hasTime && (limiter as LeakyBucketMeterRateLimiter).tryAcquire(capacityFor(deadline, now()))
        } else {
            limiter.tick()
        }
        metrics.incomingDecision(accepted)
        return accepted
    }

    internal fun capacityFor(deadline: Long, now: Long): Int {
        return (bufferMillis(deadline, now) * account.rateLimitPerSec / 1000L)
            .coerceIn(1L, maxBufferedRequests.toLong()).toInt()
    }

    private fun bufferMillis(deadline: Long, now: Long) =
        maxOf(1000L, deadline - now - account.averageProcessingTime.toMillis() - 1000L)
}
