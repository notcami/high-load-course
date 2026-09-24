package ru.quipy.payments.logic

import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class AccountAdmission(ratePerSecond: Int, parallelRequests: Int) {
    init {
        require(ratePerSecond > 0 && parallelRequests > 0)
    }

    private val intervalNanos = (1_050_000_000L + ratePerSecond - 1) / ratePerSecond
    private val slots = Semaphore(parallelRequests, true)
    private val pacing = ReentrantLock(true)
    private var nextStart = System.nanoTime()

    @Throws(InterruptedException::class)
    fun acquire(deadline: Long, responseReserve: Duration): Boolean {
        val budgetMillis = deadline - System.currentTimeMillis() - responseReserve.toMillis()
        if (budgetMillis <= 0) return false
        val end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMillis)
        if (!slots.tryAcquire(budgetMillis, TimeUnit.MILLISECONDS)) return false
        var admitted = false
        try {
            val lockBudget = end - System.nanoTime()
            if (lockBudget <= 0 || !pacing.tryLock(lockBudget, TimeUnit.NANOSECONDS)) return false
            try {
                val wait = nextStart - System.nanoTime()
                if (wait > 0) {
                    if (wait >= end - System.nanoTime()) return false
                    TimeUnit.NANOSECONDS.sleep(wait)
                }
                if (System.nanoTime() >= end) return false
                nextStart = System.nanoTime() + intervalNanos
                admitted = true
                return true
            } finally {
                pacing.unlock()
            }
        } finally {
            if (!admitted) slots.release()
        }
    }

    fun release() = slots.release()
}
