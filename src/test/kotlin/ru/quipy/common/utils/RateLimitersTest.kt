package ru.quipy.common.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class RateLimitersTest {
    @Test
    fun `sliding window expires individual requests rather than resetting a fixed window`() {
        var nanos = 0L
        val limiter = SlidingWindowRateLimiter(2, Duration.ofSeconds(1)) { nanos }
        assertTrue(limiter.tick())
        nanos = TimeUnit.MILLISECONDS.toNanos(500)
        assertTrue(limiter.tick())
        nanos = TimeUnit.MILLISECONDS.toNanos(999)
        assertFalse(limiter.tick())
        nanos = TimeUnit.SECONDS.toNanos(1)
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
        nanos = TimeUnit.MILLISECONDS.toNanos(1500)
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
    }

    @Test
    fun `parallel callers cannot exceed the sliding window limit`() {
        val limiter = SlidingWindowRateLimiter(11, Duration.ofSeconds(1)) { 0L }
        val executor = Executors.newFixedThreadPool(16)
        try {
            val results = executor.invokeAll(List(1000) { Callable { limiter.tick() } })
            assertEquals(11, results.count { it.get() })
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `meter accepts a burst then drains continuously without counting rejections`() {
        var nanos = 0L
        val limiter = LeakyBucketMeterRateLimiter(2, 4) { nanos }
        repeat(4) { assertTrue(limiter.tick()) }
        repeat(100) { assertFalse(limiter.tick()) }
        nanos = TimeUnit.MILLISECONDS.toNanos(499)
        assertFalse(limiter.tick())
        nanos = TimeUnit.MILLISECONDS.toNanos(500)
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
        nanos = TimeUnit.SECONDS.toNanos(3)
        repeat(4) { assertTrue(limiter.tick()) }
        assertFalse(limiter.tick())
    }

    @Test
    fun `different deadline budgets share one meter`() {
        var nanos = 0L
        val limiter = LeakyBucketMeterRateLimiter(11, 192) { nanos }
        repeat(20) { assertTrue(limiter.tryAcquire(121)) }
        assertFalse(limiter.tryAcquire(11))
        nanos = TimeUnit.SECONDS.toNanos(1)
        assertTrue(limiter.tryAcquire(11))
        assertTrue(limiter.tryAcquire(11))
        assertFalse(limiter.tryAcquire(11))
    }

    @Test
    fun `meter sustains all 11 permits per second after a burst`() {
        var nanos = 0L
        val limiter = LeakyBucketMeterRateLimiter(11, 110) { nanos }
        repeat(110) { assertTrue(limiter.tick()) }
        repeat(200) {
            nanos += TimeUnit.SECONDS.toNanos(1)
            repeat(11) { assertTrue(limiter.tick()) }
            assertFalse(limiter.tick())
        }
    }

    @Test
    fun `meter uses the same guarded period as the provider window`() {
        var nanos = 0L
        val period = Duration.ofMillis(1005)
        val limiter = LeakyBucketMeterRateLimiter(11, 11, period) { nanos }
        repeat(11) { assertTrue(limiter.tick()) }
        repeat(200) {
            nanos += period.toNanos()
            repeat(11) { assertTrue(limiter.tick()) }
            assertFalse(limiter.tick())
        }
    }

    @Test
    fun `invalid limiter settings fail at startup`() {
        assertThrows(IllegalArgumentException::class.java) { SlidingWindowRateLimiter(0, Duration.ofSeconds(1)) }
        assertThrows(IllegalArgumentException::class.java) { SlidingWindowRateLimiter(1, Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { LeakyBucketMeterRateLimiter(0, 11) }
        assertThrows(IllegalArgumentException::class.java) { LeakyBucketMeterRateLimiter(11, 0) }
        assertThrows(IllegalArgumentException::class.java) { LeakyBucketMeterRateLimiter(11, 11, Duration.ZERO) }
    }
}
