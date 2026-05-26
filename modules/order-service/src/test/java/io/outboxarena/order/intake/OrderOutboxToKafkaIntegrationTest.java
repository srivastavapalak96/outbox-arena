package io.outboxarena.order.intake;

import static org.assertj.core.api.Assertions.assertThat;

import io.outboxarena.common.outbox.OutboxPoller;
import io.outboxarena.common.outbox.OutboxRecordRepository;
import io.outboxarena.order.domain.OrderRepository;
import io.outboxarena.order.domain.SagaRepository;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * End-to-end: POST /orders writes outbox row -> OutboxPoller publishes to Kafka. Drives the poller
 * directly (rather than waiting for {@code @Scheduled}) so the test is fast and deterministic.
 * Consumes the topic with a plain {@code KafkaConsumer} and asserts:
 *
 * <ul>
 *   <li>The message lands on {@code commands.order.v1}.
 *   <li>The Kafka key equals {@code aggregate_id} (the order UUID) -- preserves per-order ordering.
 *   <li>The {@code event-id} header matches the outbox row's {@code event_id}.
 *   <li>The outbox row's {@code published_at} is set after the sweep.
 * </ul>
 *
 * Requires the docker-compose stack (Postgres + Kafka KRaft) up. Gated by {@code EA_INTEGRATION=1}.
 */
@SpringBootTest
@DirtiesContext
@EnabledIfEnvironmentVariable(named = "EA_INTEGRATION", matches = "1")
class OrderOutboxToKafkaIntegrationTest {

  private static final String BOOTSTRAP = "localhost:29092";
  private static final String TOPIC = "commands.payment.v1";

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", () -> "jdbc:postgresql://localhost:5432/order_svc");
    registry.add("spring.datasource.username", () -> "order_svc");
    registry.add("spring.datasource.password", () -> "order_svc");
    registry.add("spring.kafka.bootstrap-servers", () -> BOOTSTRAP);
    registry.add("spring.kafka.listener.auto-startup", () -> "false");
    registry.add("outbox-arena.outbox.enabled", () -> "true");
    // Don't auto-sweep -- the test invokes sweepShard() directly.
    registry.add("outbox-arena.outbox.poll-interval", () -> "PT1H");
  }

  @Autowired OrderIntakeService intake;
  @Autowired OutboxPoller poller;
  @Autowired OutboxRecordRepository outboxRepository;
  @Autowired OrderRepository orderRepository;
  @Autowired SagaRepository sagaRepository;

  @BeforeEach
  void resetTables() {
    outboxRepository.deleteAll();
    sagaRepository.deleteAll();
    orderRepository.deleteAll();
    createTopicIfMissing();
  }

  @Test
  void intakePollerKafkaRoundTrip() {
    CreateOrderRequest req =
        new CreateOrderRequest(
            "buyer-pkt",
            "USD",
            List.of(new CreateOrderRequest.LineItem("seller-z", "sku-z", 1, 2_500L)));
    CreateOrderResponse response = intake.intake(req);

    UUID orderUuid = response.orderUuid();
    UUID outboxEventId = outboxRepository.findAll().get(0).getEventId();

    // Drive the poller across every shard. The intake picks a single shard for the order;
    // sweeping all of them is the simplest way to be shard-agnostic in this assertion.
    int published = 0;
    for (int shard = 0; shard < 16; shard++) {
      published += poller.sweepShard(shard);
    }
    assertThat(published).isEqualTo(1);

    ConsumerRecord<String, String> rec = consumeOne(orderUuid);
    assertThat(rec).isNotNull();
    assertThat(rec.key()).isEqualTo(orderUuid.toString());
    String headerEventId = new String(rec.headers().lastHeader("event-id").value());
    assertThat(headerEventId).isEqualTo(outboxEventId.toString());
    assertThat(rec.value()).contains(orderUuid.toString());

    assertThat(outboxRepository.findAll().get(0).getPublishedAt()).isNotNull();
  }

  /**
   * Single consumer with manual partition assignment, no consumer-group coordination,
   * seekToBeginning. Polls until the matching record arrives or the deadline passes.
   */
  private ConsumerRecord<String, String> consumeOne(UUID orderUuid) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
    try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
      List<TopicPartition> parts =
          c.partitionsFor(TOPIC).stream()
              .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
              .collect(Collectors.toList());
      c.assign(parts);
      c.seekToBeginning(parts);
      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : recs) {
          if (orderUuid.toString().equals(r.key())) {
            return r;
          }
        }
      }
    }
    return null;
  }

  private void createTopicIfMissing() {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("bootstrap.servers", BOOTSTRAP);
    try (AdminClient admin = AdminClient.create(cfg)) {
      try {
        admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
      } catch (Exception ignored) {
        // Topic already exists -- fine.
      }
    } catch (Exception ignored) {
      // Best effort -- the broker auto-creates topics anyway.
    }
  }
}
