package io.outboxarena.order.intake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.outboxarena.common.events.EventTypes;
import io.outboxarena.common.events.PaymentEvents;
import io.outboxarena.common.outbox.OutboxRecord;
import io.outboxarena.common.outbox.OutboxRecordRepository;
import io.outboxarena.order.domain.Order;
import io.outboxarena.order.domain.OrderItem;
import io.outboxarena.order.domain.OrderRepository;
import io.outboxarena.order.domain.Saga;
import io.outboxarena.order.domain.SagaRepository;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Atomic intake: writes the order row, all its order_items rows, the saga state row, AND the
 * OrderCreated outbox row inside a single database transaction. If any one fails, all roll back
 * together. This is the application-layer guarantee that makes Outbox solve the dual-write problem
 * -- the broker publish is now a downstream consequence of a single durable write rather than a
 * second independent write.
 *
 * <p>See docs/adr/0002 for the boundary: OrderCreated is a *command-plane* event that triggers the
 * saga. CDC will tail the orders table separately for read projections.
 */
@Service
public class OrderIntakeService {

  static final String OUTBOX_AGGREGATE_TYPE = "Payment";
  private static final int OUTBOX_SHARDS = 16;

  private final OrderRepository orderRepository;
  private final SagaRepository sagaRepository;
  private final OutboxRecordRepository outboxRepository;
  private final ObjectMapper objectMapper;

  public OrderIntakeService(
      OrderRepository orderRepository,
      SagaRepository sagaRepository,
      OutboxRecordRepository outboxRepository,
      ObjectMapper objectMapper) {
    this.orderRepository = orderRepository;
    this.sagaRepository = sagaRepository;
    this.outboxRepository = outboxRepository;
    this.objectMapper = objectMapper;
  }

  @Transactional
  public CreateOrderResponse intake(CreateOrderRequest request) {
    UUID orderUuid = UUID.randomUUID();
    Order order = new Order(orderUuid, request.buyerId(), request.currency());
    for (CreateOrderRequest.LineItem line : request.items()) {
      order.addItem(new OrderItem(line.sellerId(), line.sku(), line.qty(), line.unitPriceCents()));
    }
    Order persisted = orderRepository.save(order);

    Saga saga = new Saga(UUID.randomUUID(), persisted.getId());
    sagaRepository.save(saga);

    // First saga step: ask payment-service to authorise. The outbox poller routes by
    // aggregate_type -> commands.payment.v1 (see OutboxPoller.topicFor).
    PaymentEvents.PaymentRequested paymentRequest =
        new PaymentEvents.PaymentRequested(
            orderUuid, persisted.getTotalAmountCents(), persisted.getCurrency());
    OutboxRecord record =
        new OutboxRecord(
            UUID.randomUUID(),
            OUTBOX_AGGREGATE_TYPE,
            orderUuid.toString(),
            EventTypes.PAYMENT_REQUESTED,
            serialise(paymentRequest),
            shardKeyFor(orderUuid));
    outboxRepository.save(record);

    return new CreateOrderResponse(
        persisted.getOrderUuid(), persisted.getStatus(), persisted.getTotalAmountCents());
  }

  private String serialise(Object payload) {
    try {
      return objectMapper.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Failed to serialise " + payload.getClass().getSimpleName(), e);
    }
  }

  /** Partition the outbox across shards so multiple pollers can run without contention. */
  static short shardKeyFor(UUID orderUuid) {
    return (short) (Math.floorMod(orderUuid.hashCode(), OUTBOX_SHARDS));
  }
}
