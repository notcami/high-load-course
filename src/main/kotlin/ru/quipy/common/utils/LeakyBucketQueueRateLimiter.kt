package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class LeakyBucketQueueRateLimiter(
    ratePerSecond: Int,
    private val nanoTime: () -> Long = System::nanoTime,
    private val sleepNanos: (Long) -> Unit = TimeUnit.NANOSECONDS::sleep,
) {
    init {
        require(ratePerSecond > 0)
    }

    private val intervalNanos = (TimeUnit.SECONDS.toNanos(1) + ratePerSecond - 1L) / ratePerSecond
    private val maxCatchUpNanos = minOf(intervalNanos / 10, TimeUnit.MILLISECONDS.toNanos(10))
    private val lock = ReentrantLock(true)
    private var nextSendNanos = nanoTime()

    @Throws(InterruptedException::class)
    fun tryAcquire(maxWait: Duration): Boolean {
        require(!maxWait.isNegative)
        val end = nanoTime() + maxWait.toNanos()
        if (!lock.tryLock(maxOf(0L, end - nanoTime()), TimeUnit.NANOSECONDS)) return false
        try {
            while (true) {
                val now = nanoTime()
                if (now > end) return false
                val wait = nextSendNanos - now
                if (wait <= 0) {
                    nextSendNanos = if (now - nextSendNanos >= intervalNanos) {
                        now + intervalNanos
                    } else {
                        maxOf(nextSendNanos + intervalNanos, now + intervalNanos - maxCatchUpNanos)
                    }
                    return true
                }
                if (wait > end - now) return false
                sleepNanos(wait)
            }
        } finally {
            lock.unlock()
        }
    }
}
