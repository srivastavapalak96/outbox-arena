package io.outboxarena.order.intake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.outboxarena.common.outbox.OutboxRecord;
import io.outboxarena.common.outbox.OutboxRecordRepository;
import io.outboxarena.order.domain.Order;
import io.outboxarena.order.domain.OrderRepository;
import io.outboxarena.order.domain.OrderStatus;
import io.outboxarena.order.domain.SagaRepository;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Proves the central Week 2 claim: a successful intake writes the order row, all order_items rows,
 * the saga state row, AND one outbox row in a single committed transaction. A failure inside the
 * transaction (a poisonous row that violates a domain invariant) must roll back every write -- no
 * half-state, no orphan outbox.
 *
 * <p>Runs against the {@code docker compose} Postgres on localhost:5432 (database {@code
 * order_svc}, role {@code order_svc}). Gated by {@code EA_INTEGRATION=1} so unit-only Gradle runs
 * stay fast. Local: {@code make up && EA_INTEGRATION=1 ./gradlew test}. The same gate applies in CI
 * -- the workflow brings the compose stack up before invoking tests.
 *
 * <p>Testcontainers was the original plan; it's blocked locally by a docker-java + Docker 29.2.1
 * incompatibility (the engine {@code /info} probe returns a degenerate payload that docker-java
 * rejects with HTTP 400). We pin to compose-based integration tests until Testcontainers ships a
 * docker-java release that handles the new API shape.
 */
@SpringBootTest
@DirtiesContext
@EnabledIfEnvironmentVariable(named = "EA_INTEGRATION", matches = "1")
class OrderIntakeIntegrationTest {

  @DynamicPropertySource
  static void datasourceProps(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", () -> "jdbc:postgresql://localhost:5432/order_svc");
    registry.add("spring.datasource.username", () -> "order_svc");
    registry.add("spring.datasource.password", () -> "order_svc");
    registry.add("spring.flyway.clean-disabled", () -> "false");
    registry.add("spring.kafka.bootstrap-servers", () -> "localhost:29092");
    registry.add("spring.kafka.listener.auto-startup", () -> "false");
    // Keep the outbox auto-configuration ON so IdempotentConsumer / OutboxBacklogGauge
    // beans (needed by OrderSagaOrchestrator + SagaMetrics) are present. The scheduler
    // is muted with a 1-hour poll interval so it never sweeps during the test -- we're
    // exercising the atomic write path only here, not the Kafka publish.
    registry.add("outbox-arena.outbox.enabled", () -> "true");
    registry.add("outbox-arena.outbox.poll-interval", () -> "PT1H");
  }

  @Autowired OrderIntakeService intake;
  @Autowired OrderRepository orderRepository;
  @Autowired SagaRepository sagaRepository;
  @Autowired OutboxRecordRepository outboxRepository;

  @BeforeEach
  void resetTables() {
    outboxRepository.deleteAll();
    sagaRepository.deleteAll();
    orderRepository.deleteAll();
  }

  @Test
  void happyPathWritesOrderItemsSagaAndOutboxAtomically() {
    CreateOrderRequest req =
        new CreateOrderRequest(
            "buyer-42",
            "USD",
            List.of(
                new CreateOrderRequest.LineItem("seller-a", "sku-1", 2, 1_500L),
                new CreateOrderRequest.LineItem("seller-b", "sku-2", 1, 9_900L)));

    CreateOrderResponse response = intake.intake(req);

    assertThat(response.orderUuid()).isNotNull();
    assertThat(response.status()).isEqualTo(OrderStatus.PENDING);
    assertThat(response.totalAmountCents()).isEqualTo(2 * 1_500L + 1 * 9_900L);

    assertThat(orderRepository.count()).isEqualTo(1);
    assertThat(sagaRepository.count()).isEqualTo(1);
    assertThat(outboxRepository.count()).isEqualTo(1);

    Order persisted = orderRepository.findWithItemsByOrderUuid(response.orderUuid()).orElseThrow();
    assertThat(persisted.getItems()).hasSize(2);
    assertThat(persisted.getItems()).extracting("sku").containsExactlyInAnyOrder("sku-1", "sku-2");

    OutboxRecord onlyOutbox = outboxRepository.findAll().get(0);
    assertThat(onlyOutbox.getAggregateType()).isEqualTo("Payment");
    assertThat(onlyOutbox.getAggregateId()).isEqualTo(response.orderUuid().toString());
    assertThat(onlyOutbox.getEventType()).isEqualTo("PaymentRequested");
    assertThat(onlyOutbox.getPublishedAt()).isNull();
    assertThat(onlyOutbox.getPayload()).contains(response.orderUuid().toString());
  }

  @Test
  void validationFailureLeavesNoPartialState() {
    long ordersBefore = orderRepository.count();
    long sagasBefore = sagaRepository.count();
    long outboxBefore = outboxRepository.count();

    assertThatThrownBy(
            () ->
                intake.intake(
                    new CreateOrderRequest(
                        "buyer-99",
                        "USD",
                        List.of(new CreateOrderRequest.LineItem("seller-x", "sku-x", 0, 100L)))))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(orderRepository.count()).isEqualTo(ordersBefore);
    assertThat(sagaRepository.count()).isEqualTo(sagasBefore);
    assertThat(outboxRepository.count()).isEqualTo(outboxBefore);
  }

  @Test
  void shardKeyIsStableAndInRange() {
    java.util.UUID u = java.util.UUID.fromString("00000000-0000-0000-0000-000000000001");
    short first = OrderIntakeService.shardKeyFor(u);
    short second = OrderIntakeService.shardKeyFor(u);
    assertThat(first).isEqualTo(second);
    assertThat(first).isBetween((short) 0, (short) 15);
  }
}
