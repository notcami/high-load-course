package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.util.UUID

@Service
class PaymentSystemImpl(paymentAccounts: List<PaymentExternalSystemAdapter>) : PaymentService {
    private val account = paymentAccounts.filter { it.isEnabled() }.singleOrNull()
        ?: error("Case 1 requires exactly one enabled payment account (acc-3)")

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
    }
}
