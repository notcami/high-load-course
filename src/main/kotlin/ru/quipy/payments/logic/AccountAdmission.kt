package ru.quipy.payments.logic

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class AccountAdmission(private val ratePerSecond: Int, parallelRequests: Int) {
    init {
        require(ratePerSecond > 0 && parallelRequests > 0)
    }

    private val windowNanos = TimeUnit.SECONDS.toNanos(1)
    private val slots = Semaphore(parallelRequests, true)
    private val windowLock = ReentrantLock(true)
    private val starts = ArrayDeque<Long>()

    @Throws(InterruptedException::class)
    fun acquire(deadline: Long, responseReserve: Duration): Boolean {
        val budgetMillis = deadline - System.currentTimeMillis() - responseReserve.toMillis()
        if (budgetMillis <= 0) return false
        val end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMillis)
        if (!slots.tryAcquire(budgetMillis, TimeUnit.MILLISECONDS)) return false
        var admitted = false
        try {
            val lockBudget = end - System.nanoTime()
            if (lockBudget <= 0 || !windowLock.tryLock(lockBudget, TimeUnit.NANOSECONDS)) return false
            try {
                while (true) {
                    val now = System.nanoTime()
                    if (now >= end) return false
                    while (starts.isNotEmpty() && now - starts.peekFirst() >= windowNanos) {
                        starts.removeFirst()
                    }
                    if (starts.size < ratePerSecond) {
                        starts.addLast(now)
                        admitted = true
                        return true
                    }
                    val wait = windowNanos - (now - starts.peekFirst())
                    if (wait >= end - now) return false
                    TimeUnit.NANOSECONDS.sleep(wait)
                }
            } finally {
                windowLock.unlock()
            }
        } finally {
            if (!admitted) slots.release()
        }
    }

    fun release() = slots.release()
}
