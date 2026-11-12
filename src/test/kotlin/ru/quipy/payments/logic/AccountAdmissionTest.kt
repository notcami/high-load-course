package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class AccountAdmissionTest {
    @Test
    fun `all 11 provider permits remain available in every window after idle`() {
        var nanos = 0L
        val admission = AccountAdmission(11, 64, { nanos }, { nanos += it })
        val deadline = System.currentTimeMillis() + 26_000
        val sent = List(22) {
            assertTrue(send(admission, deadline))
            nanos
        }
        assertTrue(sent.take(11).all { it == 0L })
        val window = TimeUnit.MILLISECONDS.toNanos(1005)
        for (start in sent) {
            assertTrue(sent.count { it >= start && it < start + window } <= 11)
        }
        nanos += TimeUnit.SECONDS.toNanos(5)
        val afterIdle = nanos
        repeat(11) { assertTrue(send(admission, deadline)) }
        assertEquals(afterIdle, nanos)
        assertFalse(send(admission, System.currentTimeMillis() + 40))
    }

    @Test
    fun `deadline rejection does not consume a future permit`() {
        var nanos = 0L
        val admission = AccountAdmission(11, 64, { nanos }, { nanos += it })
        repeat(11) { assertTrue(send(admission, System.currentTimeMillis() + 1000)) }
        assertFalse(send(admission, System.currentTimeMillis() + 40))
        assertTrue(send(admission, System.currentTimeMillis() + 2000))
        assertEquals(TimeUnit.MILLISECONDS.toNanos(1005), nanos)
        repeat(10) { assertTrue(send(admission, System.currentTimeMillis() + 2000)) }
        assertFalse(send(admission, System.currentTimeMillis() + 40))
    }

    @Test
    fun `late wakeup cannot create extra provider window permits`() {
        var nanos = 0L
        var oversleep = TimeUnit.MILLISECONDS.toNanos(40)
        val admission = AccountAdmission(11, 64, { nanos }) {
            nanos += it + oversleep
            oversleep = 0
        }
        val deadline = System.currentTimeMillis() + 26_000
        repeat(11) { assertTrue(send(admission, deadline)) }
        assertTrue(send(admission, deadline))
        val delayed = nanos
        repeat(10) { assertTrue(send(admission, deadline)) }
        assertEquals(delayed, nanos)
        assertTrue(send(admission, deadline))
        assertTrue(nanos - delayed >= TimeUnit.MILLISECONDS.toNanos(1005))
    }

    @Test
    fun `provider windows with delayed reset stay within the limit at different phases`() {
        val providerWindow = TimeUnit.MILLISECONDS.toNanos(1005)
        for (phaseMillis in listOf(0L, 100L, 500L, 990L)) {
            var nanos = 0L
            val admission = AccountAdmission(11, 64, { nanos }, { nanos += it })
            val deadline = System.currentTimeMillis() + 26_000
            val sent = List(220) {
                assertTrue(send(admission, deadline))
                nanos
            }
            val phase = TimeUnit.MILLISECONDS.toNanos(phaseMillis)
            val windows = sent.groupingBy { (it + phase) / providerWindow }.eachCount()
            assertTrue(windows.values.all { it <= 11 })
            assertTrue((sent.size - 1) * 1_000_000_000.0 / (sent.last() - sent.first()) >= 10.5)
        }
    }

    @Test
    fun `window starts at the actual send after a connection delay`() {
        var nanos = 0L
        val admission = AccountAdmission(11, 64, { nanos }, { nanos += it })
        val deadline = System.currentTimeMillis() + 26_000
        assertTrue(admission.acquireRatePermit(deadline, Duration.ZERO))
        nanos += TimeUnit.MILLISECONDS.toNanos(200)
        admission.releaseRatePermit()
        val sentAt = nanos
        repeat(10) { assertTrue(send(admission, deadline)) }
        assertEquals(sentAt, nanos)
        assertTrue(send(admission, deadline))
        assertTrue(nanos - sentAt >= TimeUnit.MILLISECONDS.toNanos(1005))
    }

    @Test
    fun `another thread cannot send before the previous request is flushed`() {
        val admission = AccountAdmission(11, 64)
        val deadline = System.currentTimeMillis() + 5000
        val started = CountDownLatch(1)
        val sent = CountDownLatch(1)
        val executor = Executors.newSingleThreadExecutor()
        assertTrue(admission.acquireRatePermit(deadline, Duration.ZERO))
        try {
            val next = executor.submit<Boolean> {
                started.countDown()
                val accepted = admission.acquireRatePermit(deadline, Duration.ZERO)
                if (accepted) {
                    sent.countDown()
                    admission.releaseRatePermit()
                }
                accepted
            }
            assertTrue(started.await(1, TimeUnit.SECONDS))
            assertFalse(sent.await(50, TimeUnit.MILLISECONDS))
            admission.releaseRatePermit()
            assertTrue(next.get(2, TimeUnit.SECONDS))
        } finally {
            admission.releaseRatePermit()
            executor.shutdownNow()
        }
    }

    private fun send(admission: AccountAdmission, deadline: Long): Boolean {
        val accepted = admission.acquireRatePermit(deadline, Duration.ZERO)
        if (accepted) admission.releaseRatePermit()
        return accepted
    }
}
