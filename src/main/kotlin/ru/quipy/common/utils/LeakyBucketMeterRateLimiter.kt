package ru.quipy.common.utils

import java.time.Duration

class LeakyBucketMeterRateLimiter(
    private val ratePerSecond: Int,
    private val capacity: Int,
    private val period: Duration = Duration.ofSeconds(1),
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    init {
        require(ratePerSecond > 0 && capacity > 0) { "Rate and capacity must be positive" }
        require(!period.isNegative && !period.isZero)
    }

    private var level = 0.0
    private var measuredAt = nanoTime()

    @Synchronized
    override fun tick(): Boolean = tryAcquire(capacity)

    @Synchronized
    fun tryAcquire(capacityLimit: Int): Boolean {
        require(capacityLimit in 1..capacity)
        val now = nanoTime()
        val drained = (now - measuredAt).toDouble() / period.toNanos() * ratePerSecond
        level = maxOf(0.0, level - drained)
        measuredAt = now
        if (level + 1 > capacityLimit + 1e-9) return false
        level += 1
        return true
    }
}
