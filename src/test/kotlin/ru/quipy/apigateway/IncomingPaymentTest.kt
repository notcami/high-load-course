package ru.quipy.apigateway

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.http.HttpStatus
import org.springframework.test.util.ReflectionTestUtils
import ru.quipy.common.utils.LeakyBucketQueueRateLimiter
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.*
import java.time.Duration
import java.util.UUID

class IncomingPaymentTest {
    private val registry = SimpleMeterRegistry()
    private val metrics = PaymentMetrics(registry, "test-shop")
    private val account = PaymentAccountProperties("test-shop", "acc-23", 64, 11, 30, Duration.ofSeconds(1), true)

    @Test
    fun `429 includes Retry-After and does not start or change an order`() {
        val repository = mock(OrderRepository::class.java)
        val payer = mock(OrderPayer::class.java)
        val limiter = IncomingPaymentLimiter(account, metrics, "sliding-window", 11, 1, 128, 64)
        val deadline = now() + 26_000
        repeat(11) { assertTrue(limiter.tryAcquire(deadline)) }

        val orderId = UUID.randomUUID()
        val order = APIController.Order(orderId, UUID.randomUUID(), now(), APIController.OrderStatus.COLLECTING, 400)
        `when`(repository.findById(orderId)).thenReturn(order)
        val controller = APIController()
        ReflectionTestUtils.setField(controller, "orderRepository", repository)
        ReflectionTestUtils.setField(controller, "orderPayer", payer)
        ReflectionTestUtils.setField(controller, "incomingPaymentLimiter", limiter)

        val response = controller.payOrder(orderId, deadline)

        assertEquals(HttpStatus.TOO_MANY_REQUESTS, response.statusCode)
        assertEquals("1", response.headers.getFirst("Retry-After"))
        assertNull(response.body)
        verify(repository).findById(orderId)
        verifyNoMoreInteractions(repository)
        verifyNoInteractions(payer)
        assertEquals(1.0, registry.get("online.shop.payment.ingress").tag("outcome", "REJECTED").counter().count())
    }

    @Test
    fun `accepted requests preserve the payment submission response`() {
        val repository = mock(OrderRepository::class.java)
        val payer = mock(OrderPayer::class.java)
        val limiter = IncomingPaymentLimiter(account, metrics, "deadline-aware", 11, 1, 128, 64)
        val orderId = UUID.randomUUID()
        val order = APIController.Order(orderId, UUID.randomUUID(), now(), APIController.OrderStatus.COLLECTING, 400)
        `when`(repository.findById(orderId)).thenReturn(order)
        val controller = APIController()
        ReflectionTestUtils.setField(controller, "orderRepository", repository)
        ReflectionTestUtils.setField(controller, "orderPayer", payer)
        ReflectionTestUtils.setField(controller, "incomingPaymentLimiter", limiter)

        val deadline = now() + 13_000
        val response = controller.payOrder(orderId, deadline)

        assertEquals(HttpStatus.OK, response.statusCode)
        assertNotNull(response.body?.transactionId)
        verify(repository).save(order.copy(status = APIController.OrderStatus.PAYMENT_IN_PROGRESS))
        verify(payer).processPayment(orderId, 400, response.body!!.transactionId, deadline)
        assertEquals(1.0, registry.get("online.shop.payment.ingress").tag("outcome", "ACCEPTED").counter().count())
    }

    @Test
    fun `deadline capacity grows for bursts but fits the existing executor`() {
        val limiter = IncomingPaymentLimiter(account, metrics, "deadline-aware", 11, 1, 128, 64)
        assertEquals(11, limiter.capacityFor(2500, 0))
        assertEquals(121, limiter.capacityFor(13000, 0))
        assertEquals(192, limiter.capacityFor(26000, 0))
        assertEquals(11, limiter.capacityFor(0, 0))
    }

    @Test
    fun `waiting before the handler does not bypass the queued admission mode`() {
        val repository = mock(OrderRepository::class.java)
        val payer = mock(OrderPayer::class.java)
        val limiter = IncomingPaymentLimiter(account, metrics, "deadline-aware", 11, 1, 128, 64)
        ReflectionTestUtils.setField(limiter, "incomingQueue", mock(LeakyBucketQueueRateLimiter::class.java))
        val at = now()
        val orderId = UUID.randomUUID()
        val order = APIController.Order(orderId, UUID.randomUUID(), at - 7000, APIController.OrderStatus.COLLECTING, 400)
        `when`(repository.findById(orderId)).thenReturn(order)
        val controller = APIController()
        ReflectionTestUtils.setField(controller, "orderRepository", repository)
        ReflectionTestUtils.setField(controller, "orderPayer", payer)
        ReflectionTestUtils.setField(controller, "incomingPaymentLimiter", limiter)

        val response = controller.payOrder(orderId, at + 19_000)

        assertEquals(HttpStatus.TOO_MANY_REQUESTS, response.statusCode)
        verify(repository).findById(orderId)
        verifyNoMoreInteractions(repository)
        verifyNoInteractions(payer)
    }
}
