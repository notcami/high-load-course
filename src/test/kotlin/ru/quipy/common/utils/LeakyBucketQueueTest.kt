package ru.quipy.common.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class LeakyBucketQueueTest {
    @Test
    fun `concurrent requests wait for separate permits`() {
        val nanos = AtomicLong()
        val limiter = LeakyBucketQueueRateLimiter(100, nanos::get) { nanos.addAndGet(it) }
        val executor = Executors.newFixedThreadPool(8)
        val start = CountDownLatch(1)
        try {
            val requests = List(8) {
                executor.submit<Boolean> {
                    start.await()
                    limiter.tryAcquire(Duration.ofSeconds(1))
                }
            }
            start.countDown()
            requests.forEach { assertTrue(it.get(2, TimeUnit.SECONDS)) }
            assertTrue(nanos.get() >= TimeUnit.MILLISECONDS.toNanos(70))
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `insufficient waiting budget does not consume a permit`() {
        var nanos = 0L
        val limiter = LeakyBucketQueueRateLimiter(11, { nanos }, { nanos += it })
        assertTrue(limiter.tryAcquire(Duration.ofSeconds(1)))
        assertFalse(limiter.tryAcquire(Duration.ofMillis(40)))
        assertEquals(0L, nanos)
        assertTrue(limiter.tryAcquire(Duration.ofSeconds(1)))
        assertTrue(nanos in TimeUnit.MILLISECONDS.toNanos(90)..TimeUnit.MILLISECONDS.toNanos(100))
    }

    @Test
    fun `idle time does not allow queued requests to leave together`() {
        var nanos = 0L
        val limiter = LeakyBucketQueueRateLimiter(11, { nanos }, { nanos += it })
        assertTrue(limiter.tryAcquire(Duration.ofSeconds(1)))
        nanos += TimeUnit.SECONDS.toNanos(10)
        assertTrue(limiter.tryAcquire(Duration.ofSeconds(1)))
        val resumed = nanos
        assertTrue(limiter.tryAcquire(Duration.ofSeconds(1)))
        assertTrue(nanos - resumed >= TimeUnit.MILLISECONDS.toNanos(90))
        assertThrows(IllegalArgumentException::class.java) { limiter.tryAcquire(Duration.ofMillis(-1)) }
    }

    @Test
    fun `small timer delays do not reduce sustained throughput`() {
        var nanos = 0L
        val limiter = LeakyBucketQueueRateLimiter(11, { nanos }) {
            nanos += it + TimeUnit.MILLISECONDS.toNanos(5)
        }
        repeat(111) { assertTrue(limiter.tryAcquire(Duration.ofSeconds(1))) }
        assertTrue(nanos <= TimeUnit.MILLISECONDS.toNanos(10_010))
    }

    @Test
    fun `catching up a delayed timer cannot create a burst`() {
        var nanos = 0L
        var delay = TimeUnit.MILLISECONDS.toNanos(45)
        val limiter = LeakyBucketQueueRateLimiter(11, { nanos }) {
            nanos += it + delay
            delay = 0
        }
        assertTrue(limiter.tryAcquire(Duration.ofSeconds(1)))
        assertTrue(limiter.tryAcquire(Duration.ofSeconds(1)))
        val delayed = nanos
        assertTrue(limiter.tryAcquire(Duration.ofSeconds(1)))
        assertTrue(nanos - delayed >= TimeUnit.MILLISECONDS.toNanos(81))
    }
}
