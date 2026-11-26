package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit

@Component
class PaymentMetrics(
    private val registry: MeterRegistry,
    @param:Value("\${payment.service-name}") val serviceName: String,
) {
    enum class Outcome {
        SUCCESS, LATE_SUCCESS, DEADLINE, QUEUE_FULL, INTERRUPTED, TIMEOUT, IO_ERROR,
        HTTP_ERROR, PROVIDER_REJECTED, TEMPORARY_ERROR, RATE_LIMIT, PARALLEL_LIMIT, INTERNAL_ERROR
    }

    private val incoming = registry.counter("online.shop.payment.requests", "service", serviceName)

    fun received() = incoming.increment()

    fun incomingDecision(accepted: Boolean) = registry.counter(
        "online.shop.payment.ingress", "service", serviceName,
        "outcome", if (accepted) "ACCEPTED" else "REJECTED"
    ).increment()

    fun completed(outcome: Outcome) = registry.counter(
        "online.shop.payment.completed", "service", serviceName, "outcome", outcome.name
    ).increment()

    fun registerAccount(account: String) {
        registry.counter("online.shop.provider.started", "service", serviceName, "accountName", account)
        registry.counter("online.shop.payment.retries", "service", serviceName, "accountName", account)
    }

    fun providerStarted(account: String) = registry.counter(
        "online.shop.provider.started", "service", serviceName, "accountName", account
    ).increment()

    fun retried(account: String) = registry.counter(
        "online.shop.payment.retries", "service", serviceName, "accountName", account
    ).increment()

    fun providerFinished(account: String, outcome: Outcome, durationNanos: Long) {
        registry.counter(
            "online.shop.provider.finished", "service", serviceName,
            "accountName", account, "outcome", outcome.name
        ).increment()
        Timer.builder("online.shop.provider.attempt.duration")
            .tags("service", serviceName, "accountName", account, "outcome", outcome.name)
            .serviceLevelObjectives(
                Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofMillis(750),
                Duration.ofSeconds(1), Duration.ofMillis(1250), Duration.ofMillis(1500),
                Duration.ofMillis(1750), Duration.ofSeconds(2), Duration.ofMillis(2250),
            )
            .register(registry)
            .record(durationNanos, TimeUnit.NANOSECONDS)
    }
}
