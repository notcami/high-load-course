package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class PaymentMetrics(
    private val registry: MeterRegistry,
    @param:Value("\${payment.service-name}") val serviceName: String,
) {
    enum class Outcome {
        SUCCESS, LATE_SUCCESS, DEADLINE, QUEUE_FULL, INTERRUPTED, TIMEOUT, IO_ERROR,
        HTTP_ERROR, PROVIDER_REJECTED, RATE_LIMIT, PARALLEL_LIMIT, INTERNAL_ERROR
    }

    private val incoming = registry.counter("online.shop.payment.requests", "service", serviceName)

    fun received() = incoming.increment()
    fun completed(outcome: Outcome) = registry.counter(
        "online.shop.payment.completed", "service", serviceName, "outcome", outcome.name
    ).increment()

    fun registerAccount(account: String) {
        registry.counter("online.shop.provider.started", "service", serviceName, "accountName", account)
    }

    fun providerStarted(account: String) = registry.counter(
        "online.shop.provider.started", "service", serviceName, "accountName", account
    ).increment()

    fun providerFinished(account: String, outcome: Outcome) = registry.counter(
        "online.shop.provider.finished", "service", serviceName,
        "accountName", account, "outcome", outcome.name
    ).increment()
}
