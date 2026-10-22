package ru.quipy.payments.logic

import java.time.Duration
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class AccountAdmission(ratePerSecond: Int, parallelRequests: Int) {
    init {
        require(ratePerSecond > 0 && parallelRequests > 0)
    }

    private val rateLimiter = SlidingWindowRateLimiter(ratePerSecond.toLong(), Duration.ofSeconds(1))
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
        val end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMillis)
        val lockBudget = end - System.nanoTime()
        if (lockBudget <= 0 || !windowLock.tryLock(lockBudget, TimeUnit.NANOSECONDS)) return false
        try {
            while (true) {
                val now = System.nanoTime()
                if (now >= end) return false
                if (rateLimiter.tick()) return true
                TimeUnit.NANOSECONDS.sleep(minOf(TimeUnit.MILLISECONDS.toNanos(10), end - now))
            }
        } finally {
            windowLock.unlock()
        }
    }

    fun release() = slots.release()
}
