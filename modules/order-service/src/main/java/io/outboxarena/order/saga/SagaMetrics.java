package io.outboxarena.order.saga;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.outboxarena.order.domain.OrderRepository;
import io.outboxarena.order.domain.OrderStatus;
import org.springframework.stereotype.Component;

/**
 * Custom saga metrics. Counters track terminal outcomes and the trigger for each compensation flow.
 * The in-flight gauge is computed lazily by polling the orders table on Prometheus scrape; for a
 * tens-of-thousands of orders/sec system this would need a more sophisticated approach (registered
 * gauge with cache), but at portfolio scale a single COUNT per scrape is fine.
 */
@Component
public class SagaMetrics {

  private final Counter ordersCompleted;
  private final Counter ordersCancelled;
  private final Counter compensationPaymentFailed;
  private final Counter compensationInventoryRejected;
  private final Counter compensationShipmentFailed;
  private final Counter idempotencyDrops;

  public SagaMetrics(MeterRegistry registry, OrderRepository orderRepository) {
    this.ordersCompleted =
        Counter.builder("saga.completed.total")
            .description("Sagas that reached a terminal happy-path COMPLETED state")
            .tag("outcome", "completed")
            .register(registry);
    this.ordersCancelled =
        Counter.builder("saga.completed.total")
            .description("Sagas that reached a terminal CANCELLED state")
            .tag("outcome", "cancelled")
            .register(registry);
    this.compensationPaymentFailed =
        Counter.builder("saga.compensation.total")
            .description("Saga compensations triggered, dimensioned by what failed")
            .tag("trigger", "payment_failed")
            .register(registry);
    this.compensationInventoryRejected =
        Counter.builder("saga.compensation.total")
            .tag("trigger", "inventory_rejected")
            .register(registry);
    this.compensationShipmentFailed =
        Counter.builder("saga.compensation.total")
            .tag("trigger", "shipment_failed")
            .register(registry);
    this.idempotencyDrops =
        Counter.builder("consumer.idempotency.drops.total")
            .description("Duplicate events the consumer dropped via processed_events PK conflict")
            .register(registry);

    Gauge.builder(
            "saga.inflight.count",
            () ->
                orderRepository.countByStatusIn(
                    java.util.List.of(
                        OrderStatus.PENDING,
                        OrderStatus.PAYMENT_AUTHORIZED,
                        OrderStatus.INVENTORY_RESERVED,
                        OrderStatus.SHIPPED,
                        OrderStatus.COMPENSATING_INVENTORY,
                        OrderStatus.COMPENSATING_PAYMENT)))
        .description("Orders currently mid-saga (non-terminal status)")
        .register(registry);
  }

  void recordOrderCompleted() {
    ordersCompleted.increment();
  }

  void recordOrderCancelled() {
    ordersCancelled.increment();
  }

  void recordCompensationPaymentFailed() {
    compensationPaymentFailed.increment();
  }

  void recordCompensationInventoryRejected() {
    compensationInventoryRejected.increment();
  }

  void recordCompensationShipmentFailed() {
    compensationShipmentFailed.increment();
  }

  void recordIdempotencyDrop() {
    idempotencyDrops.increment();
  }
}
