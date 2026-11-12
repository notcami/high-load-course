package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    init {
        require(rate > 0) { "Rate must be positive" }
        require(!window.isNegative && !window.isZero) { "Window must be positive" }
    }

    private val windowNanos = window.toNanos()
    private val requests = ArrayDeque<Long>()

    @Synchronized
    override fun tick(): Boolean {
        val now = nanoTime()
        expire(now)
        if (requests.size.toLong() >= rate) return false
        requests.addLast(now)
        return true
    }

    @Synchronized
    fun nextPermitDelayNanos(): Long {
        val now = nanoTime()
        expire(now)
        return if (requests.size.toLong() < rate) 0L else windowNanos - (now - requests.first)
    }

    private fun expire(now: Long) {
        while (requests.isNotEmpty() && now - requests.first >= windowNanos) {
            requests.removeFirst()
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

}
