package io.outboxarena.order.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.outboxarena.common.events.EventTypes;
import io.outboxarena.common.events.InventoryEvents;
import io.outboxarena.common.events.OrderEvents;
import io.outboxarena.common.events.PaymentEvents;
import io.outboxarena.common.events.ShippingEvents;
import io.outboxarena.common.events.Topics;
import io.outboxarena.common.outbox.IdempotentConsumer;
import io.outboxarena.common.outbox.OutboxRecord;
import io.outboxarena.common.outbox.OutboxRecordRepository;
import io.outboxarena.order.domain.Order;
import io.outboxarena.order.domain.OrderRepository;
import io.outboxarena.order.domain.OrderStatus;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The saga orchestrator. Listens to the reply topics from each participant and advances the order's
 * status. Every transition writes the next command's outbox row in the same transaction as the
 * order-row UPDATE, preserving the dual-write atomicity at every saga step (not just intake).
 *
 * <p>Happy-path flow below; the compensation handlers (handleInventoryRejected,
 * handleShipmentFailed, handlePaymentRefunded) implement the failure-path transitions.
 *
 * <pre>
 *   PENDING ---PaymentAuthorized---> PAYMENT_AUTHORIZED
 *                                      |
 *                                      | (writes InventoryReservationRequested outbox)
 *                                      v
 *                                  INVENTORY_RESERVED <---InventoryReserved---
 *                                      |
 *                                      | (writes ShippingRequested outbox)
 *                                      v
 *                                    SHIPPED <---ShipmentDispatched---
 *                                      |
 *                                      | (writes OrderCompleted outbox)
 *                                      v
 *                                  COMPLETED
 * </pre>
 */
@Component
public class OrderSagaOrchestrator {

  private static final Logger LOG = LoggerFactory.getLogger(OrderSagaOrchestrator.class);
  private static final int OUTBOX_SHARDS = 16;
  static final String CONSUMER_GROUP = "order-service-saga";

  private final IdempotentConsumer idempotent;
  private final OrderRepository orders;
  private final OutboxRecordRepository outbox;
  private final ObjectMapper mapper;
  private final SagaMetrics metrics;

  public OrderSagaOrchestrator(
      IdempotentConsumer idempotent,
      OrderRepository orders,
      OutboxRecordRepository outbox,
      ObjectMapper mapper,
      SagaMetrics metrics) {
    this.idempotent = idempotent;
    this.orders = orders;
    this.outbox = outbox;
    this.mapper = mapper;
    this.metrics = metrics;
  }

  @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = CONSUMER_GROUP)
  public void onPaymentEvent(ConsumerRecord<String, String> record) {
    String eventType = headerOrNull(record, "event-type");
    if (eventType == null) {
      return;
    }
    UUID eventId = UUID.fromString(headerOrNull(record, "event-id"));
    switch (eventType) {
      case EventTypes.PAYMENT_AUTHORIZED ->
          idempotent.once(
              eventId,
              CONSUMER_GROUP,
              () -> {
                handlePaymentAuthorized(
                    readJson(record.value(), PaymentEvents.PaymentAuthorized.class));
                return null;
              });
      case EventTypes.PAYMENT_FAILED ->
          idempotent.once(
              eventId,
              CONSUMER_GROUP,
              () -> {
                handlePaymentFailed(readJson(record.value(), PaymentEvents.PaymentFailed.class));
                return null;
              });
      case EventTypes.PAYMENT_REFUNDED ->
          idempotent.once(
              eventId,
              CONSUMER_GROUP,
              () -> {
                handlePaymentRefunded(
                    readJson(record.value(), PaymentEvents.PaymentRefunded.class));
                return null;
              });
      default -> {
        /* not a reply we care about -- the PaymentRequested / PaymentRefundRequested we wrote */
      }
    }
  }

  @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = CONSUMER_GROUP)
  public void onInventoryEvent(ConsumerRecord<String, String> record) {
    String eventType = headerOrNull(record, "event-type");
    if (eventType == null) {
      return;
    }
    UUID eventId = UUID.fromString(headerOrNull(record, "event-id"));
    switch (eventType) {
      case EventTypes.INVENTORY_RESERVED ->
          idempotent.once(
              eventId,
              CONSUMER_GROUP,
              () -> {
                handleInventoryReserved(
                    readJson(record.value(), InventoryEvents.InventoryReserved.class));
                return null;
              });
      case EventTypes.INVENTORY_REJECTED ->
          idempotent.once(
              eventId,
              CONSUMER_GROUP,
              () -> {
                handleInventoryRejected(
                    readJson(record.value(), InventoryEvents.InventoryRejected.class));
                return null;
              });
      case EventTypes.INVENTORY_RELEASED ->
          idempotent.once(
              eventId,
              CONSUMER_GROUP,
              () -> {
                handleInventoryReleased(
                    readJson(record.value(), InventoryEvents.InventoryReleased.class));
                return null;
              });
      default -> {
        /* not a reply */
      }
    }
  }

  @KafkaListener(topics = Topics.SHIPPING_COMMANDS, groupId = CONSUMER_GROUP)
  public void onShippingEvent(ConsumerRecord<String, String> record) {
    String eventType = headerOrNull(record, "event-type");
    if (!EventTypes.SHIPMENT_DISPATCHED.equals(eventType)
        && !EventTypes.SHIPMENT_FAILED.equals(eventType)) {
      return;
    }
    UUID eventId = UUID.fromString(headerOrNull(record, "event-id"));
    idempotent.once(
        eventId,
        CONSUMER_GROUP,
        () -> {
          if (EventTypes.SHIPMENT_DISPATCHED.equals(eventType)) {
            handleShipmentDispatched(
                readJson(record.value(), ShippingEvents.ShipmentDispatched.class));
          } else {
            handleShipmentFailed(readJson(record.value(), ShippingEvents.ShipmentFailed.class));
          }
          return null;
        });
  }

  // ---------- transitions ----------

  private void handlePaymentAuthorized(PaymentEvents.PaymentAuthorized evt) {
    Order order = loadOrder(evt.orderUuid());
    order.transitionTo(OrderStatus.PAYMENT_AUTHORIZED);

    InventoryEvents.InventoryReservationRequested cmd =
        new InventoryEvents.InventoryReservationRequested(
            evt.orderUuid(),
            order.getItems().stream()
                .map(
                    i ->
                        new InventoryEvents.InventoryReservationRequested.Line(
                            i.getSellerId(), i.getSku(), i.getQty()))
                .toList());
    emit(evt.orderUuid(), "Inventory", EventTypes.INVENTORY_RESERVE_REQUESTED, cmd);
  }

  private void handlePaymentFailed(PaymentEvents.PaymentFailed evt) {
    Order order = loadOrder(evt.orderUuid());
    order.transitionTo(OrderStatus.CANCELLED);
    // No compensation needed -- nothing further than payment was attempted.
    emit(
        evt.orderUuid(),
        "Order",
        EventTypes.ORDER_CANCELLED,
        new OrderEvents.OrderCancelled(evt.orderUuid(), evt.reason()));
    metrics.recordCompensationPaymentFailed();
    metrics.recordOrderCancelled();
  }

  private void handleInventoryReserved(InventoryEvents.InventoryReserved evt) {
    Order order = loadOrder(evt.orderUuid());
    order.transitionTo(OrderStatus.INVENTORY_RESERVED);
    emit(
        evt.orderUuid(),
        "Shipping",
        EventTypes.SHIPPING_REQUESTED,
        new ShippingEvents.ShippingRequested(evt.orderUuid()));
  }

  private void handleInventoryRejected(InventoryEvents.InventoryRejected evt) {
    Order order = loadOrder(evt.orderUuid());
    order.transitionTo(OrderStatus.COMPENSATING_PAYMENT);
    metrics.recordCompensationInventoryRejected();
    // Ask payment-service to refund. The PaymentRefunded reply will land us at CANCELLED.
    emit(
        evt.orderUuid(),
        "Payment",
        EventTypes.PAYMENT_REFUND_REQUESTED,
        new PaymentEvents.PaymentRefundRequested(evt.orderUuid()));
  }

  private void handleShipmentDispatched(ShippingEvents.ShipmentDispatched evt) {
    Order order = loadOrder(evt.orderUuid());
    order.transitionTo(OrderStatus.SHIPPED);
    order.transitionTo(OrderStatus.COMPLETED);
    emit(
        evt.orderUuid(),
        "Order",
        EventTypes.ORDER_COMPLETED,
        new OrderEvents.OrderCompleted(evt.orderUuid(), null, evt.shipmentUuid()));
    metrics.recordOrderCompleted();
  }

  private void handleShipmentFailed(ShippingEvents.ShipmentFailed evt) {
    Order order = loadOrder(evt.orderUuid());
    order.transitionTo(OrderStatus.COMPENSATING_INVENTORY);
    metrics.recordCompensationShipmentFailed();
    // First release the inventory hold; then we'll request the payment refund once we
    // hear InventoryReleased back.
    emit(
        evt.orderUuid(),
        "Inventory",
        EventTypes.INVENTORY_RELEASE_REQUESTED,
        new InventoryEvents.InventoryReleaseRequested(evt.orderUuid()));
  }

  private void handleInventoryReleased(InventoryEvents.InventoryReleased evt) {
    Order order = loadOrder(evt.orderUuid());
    order.transitionTo(OrderStatus.COMPENSATING_PAYMENT);
    emit(
        evt.orderUuid(),
        "Payment",
        EventTypes.PAYMENT_REFUND_REQUESTED,
        new PaymentEvents.PaymentRefundRequested(evt.orderUuid()));
  }

  private void handlePaymentRefunded(PaymentEvents.PaymentRefunded evt) {
    Order order = loadOrder(evt.orderUuid());
    order.transitionTo(OrderStatus.CANCELLED);
    emit(
        evt.orderUuid(),
        "Order",
        EventTypes.ORDER_CANCELLED,
        new OrderEvents.OrderCancelled(evt.orderUuid(), "compensated"));
    metrics.recordOrderCancelled();
  }

  // ---------- helpers ----------

  private Order loadOrder(UUID orderUuid) {
    return orders
        .findWithItemsByOrderUuid(orderUuid)
        .orElseThrow(() -> new IllegalStateException("No order " + orderUuid));
  }

  private void emit(UUID orderUuid, String aggregateType, String eventType, Object payload) {
    OutboxRecord record =
        new OutboxRecord(
            UUID.randomUUID(),
            aggregateType,
            orderUuid.toString(),
            eventType,
            writeJson(payload),
            (short) Math.floorMod(orderUuid.hashCode(), OUTBOX_SHARDS));
    outbox.save(record);
    LOG.debug("Saga emit {} for order {}", eventType, orderUuid);
  }

  private static String headerOrNull(ConsumerRecord<String, String> record, String name) {
    var header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value());
  }

  private <T> T readJson(String json, Class<T> type) {
    try {
      return mapper.readValue(json, type);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to decode " + type.getSimpleName(), e);
    }
  }

  private String writeJson(Object o) {
    try {
      return mapper.writeValueAsString(o);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to encode " + o.getClass().getSimpleName(), e);
    }
  }
}
