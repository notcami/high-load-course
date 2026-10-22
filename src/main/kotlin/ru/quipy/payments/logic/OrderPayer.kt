package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import jakarta.annotation.PostConstruct
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Gauge
import jakarta.annotation.PreDestroy
import java.time.Duration
import java.util.concurrent.RejectedExecutionException
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    @param:Value("\${payment.executor.workers:16}") private val workers: Int,
    @param:Value("\${payment.executor.queue-capacity:256}") private val queueCapacity: Int,
) {

    init {
        require(workers > 0 && queueCapacity > 0)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        workers,
        workers,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    @Autowired
    private lateinit var metrics: PaymentMetrics

    @PostConstruct
    fun registerMetrics() {
        Gauge.builder("payment.queue.size", paymentExecutor) { it.queue.size.toDouble() }
            .tag("service", metrics.serviceName)
            .register(meterRegistry)
        Gauge.builder("payment.active.workers", paymentExecutor) { it.activeCount.toDouble() }
            .tag("service", metrics.serviceName)
            .register(meterRegistry)
        Gauge.builder("payment.queue.capacity", paymentExecutor) { queueCapacity.toDouble() }
            .tag("service", metrics.serviceName)
            .register(meterRegistry)
    }

    @PreDestroy
    fun shutdown() {
        paymentExecutor.shutdown()
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        metrics.received()
        paymentESService.create { it.create(paymentId, orderId, amount) }
        if (deadline <= now()) {
            reject(paymentId, createdAt, PaymentMetrics.Outcome.DEADLINE, "Payment deadline expired")
            return createdAt
        }
        try {
            paymentExecutor.execute {
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
        } catch (_: RejectedExecutionException) {
            reject(paymentId, createdAt, PaymentMetrics.Outcome.QUEUE_FULL, "Payment queue is full")
        }
        return createdAt
    }

    private fun reject(paymentId: UUID, createdAt: Long, outcome: PaymentMetrics.Outcome, reason: String) {
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - createdAt))
        }
        paymentESService.update(paymentId) { it.logProcessing(false, now(), transactionId, reason) }
        metrics.completed(outcome)
    }
}
