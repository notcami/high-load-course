package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
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
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(256),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    @PostConstruct
    fun registerMetrics() {
        Gauge.builder("payment.queue.size", paymentExecutor) { it.queue.size.toDouble() }
            .register(meterRegistry)
        Gauge.builder("payment.active.workers", paymentExecutor) { it.activeCount.toDouble() }
            .register(meterRegistry)
    }

    @PreDestroy
    fun shutdown() {
        paymentExecutor.shutdown()
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        paymentESService.create { it.create(paymentId, orderId, amount) }
        try {
            paymentExecutor.execute {
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
        } catch (_: RejectedExecutionException) {
            val transactionId = UUID.randomUUID()
            paymentESService.update(paymentId) {
                it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - createdAt))
            }
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, "Payment queue is full")
            }
        }
        return createdAt
    }
}
