package ru.quipy.payments.logic

import java.time.Duration
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

internal val paymentRateWindow: Duration = Duration.ofMillis(1005)

class AccountAdmission(
    ratePerSecond: Int,
    parallelRequests: Int,
    private val nanoTime: () -> Long = System::nanoTime,
    private val sleepNanos: (Long) -> Unit = TimeUnit.NANOSECONDS::sleep,
) {
    init {
        require(ratePerSecond > 0 && parallelRequests > 0)
    }

    private val rateLimiter = SlidingWindowRateLimiter(ratePerSecond.toLong(), paymentRateWindow, nanoTime)
    private val slots = Semaphore(parallelRequests, true)
    private val windowLock = ReentrantLock(true)

    @Throws(InterruptedException::class)
    fun acquireSlot(deadline: Long, responseReserve: Duration): Boolean {
        val budgetMillis = deadline - System.currentTimeMillis() - responseReserve.toMillis()
        if (budgetMillis <= 0) return false
        return slots.tryAcquire(budgetMillis, TimeUnit.MILLISECONDS)
    }

    @Throws(InterruptedException::class)
    fun acquireRatePermit(deadline: Long, responseReserve: Duration): Boolean {
        val budgetMillis = deadline - System.currentTimeMillis() - responseReserve.toMillis()
        if (budgetMillis <= 0) return false
        val end = nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMillis)
        val lockBudget = end - nanoTime()
        if (lockBudget <= 0 || !windowLock.tryLock(lockBudget, TimeUnit.NANOSECONDS)) return false
        var permitted = false
        try {
            while (true) {
                val now = nanoTime()
                if (now >= end) return false
                val wait = rateLimiter.nextPermitDelayNanos()
                if (wait <= 0) {
                    permitted = true
                    return true
                }
                sleepNanos(minOf(wait, end - now))
            }
        } finally {
            if (!permitted) windowLock.unlock()
        }
    }

    fun releaseRatePermit() {
        if (windowLock.isHeldByCurrentThread) {
            try {
                check(rateLimiter.tick())
            } finally {
                windowLock.unlock()
            }
        }
    }

    fun release() = slots.release()
}
