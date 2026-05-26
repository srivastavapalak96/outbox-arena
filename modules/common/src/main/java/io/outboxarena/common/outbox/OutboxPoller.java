package io.outboxarena.common.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Shared sharded poller. Every saga-participant service runs one of these. On a fixed cadence, for
 * each owned shard, it:
 *
 * <ol>
 *   <li>Selects up to {@code batchSize} unpublished outbox rows via {@code FOR UPDATE SKIP LOCKED}
 *       on (shard_key, id) -- this is what lets multiple poller instances scale without producing
 *       duplicates against the same row.
 *   <li>Sends each row to Kafka on topic {@code commands.<aggregate-type-lower>.v1}, with the
 *       {@code event_id} as the message key (giving consumers per-aggregate ordering) and as the
 *       {@code idempotency-key} header (for consumer-side dedup).
 *   <li>Marks the rows {@code published_at = now()} in the same transaction as the SELECT.
 * </ol>
 *
 * <p>What this does NOT solve: poller crashes between the Kafka commit and the Postgres UPDATE. On
 * the next sweep the row is still {@code published_at = NULL}, gets re-sent, and Kafka treats it as
 * a new message (different producer ID). The defence is <em>consumer-side</em> idempotency via
 * {@code processed_events} -- see ADR-0005.
 *
 * <p>Wired as a {@code @Bean} in {@link OutboxAutoConfiguration} so the entire poller is
 * conditionally absent when {@code outbox-arena.outbox.enabled=false} (used in unit tests that
 * exercise only the atomic write path).
 */
public class OutboxPoller {

  private static final Logger LOG = LoggerFactory.getLogger(OutboxPoller.class);

  private final OutboxPollerProperties props;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final MeterRegistry meterRegistry;
  private final Counter publishedCounter;
  private final Counter failureCounter;
  private final Timer publishTimer;

  @PersistenceContext private EntityManager em;

  public OutboxPoller(
      OutboxPollerProperties props,
      KafkaTemplate<String, String> kafkaTemplate,
      MeterRegistry meterRegistry) {
    this.props = props;
    this.kafkaTemplate = kafkaTemplate;
    this.meterRegistry = meterRegistry;
    this.publishedCounter =
        Counter.builder("outbox.publish.total")
            .description("Outbox rows successfully published to Kafka")
            .register(meterRegistry);
    this.failureCounter =
        Counter.builder("outbox.publish.failures")
            .description("Outbox publish failures (will retry next sweep)")
            .register(meterRegistry);
    this.publishTimer =
        Timer.builder("outbox.publish.duration")
            .description("Latency per outbox publish iteration")
            .register(meterRegistry);
  }

  @Scheduled(fixedDelayString = "${outbox-arena.outbox.poll-interval:PT0.2S}")
  public void sweep() {
    if (!props.isEnabled()) {
      return;
    }
    for (Integer shard : props.getShards()) {
      try {
        publishTimer.recordCallable(() -> sweepShard(shard));
      } catch (Exception e) {
        LOG.warn("Outbox sweep failed for shard {}: {}", shard, e.toString());
        failureCounter.increment();
      }
    }
    refreshUnpublishedGauge();
  }

  /**
   * One sweep over one shard. Public on purpose: tests drive it directly so they don't need to wait
   * for the {@code @Scheduled} interval.
   *
   * @return number of rows published in this sweep (0 if no work).
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public int sweepShard(int shard) {
    List<OutboxRecord> batch =
        em.createQuery(
                "select r from OutboxRecord r "
                    + "where r.shardKey = :shard and r.publishedAt is null "
                    + "order by r.id",
                OutboxRecord.class)
            .setParameter("shard", (short) shard)
            .setMaxResults(props.getBatchSize())
            .setHint("jakarta.persistence.lock.scope", "PESSIMISTIC_WRITE")
            .setHint("jakarta.persistence.query.timeout", 5000) // 5s safety net
            .setLockMode(jakarta.persistence.LockModeType.PESSIMISTIC_WRITE)
            .setHint("jakarta.persistence.lock.timeout", -2) // SKIP LOCKED
            .getResultList();

    if (batch.isEmpty()) {
      return 0;
    }

    for (OutboxRecord row : batch) {
      String topic = topicFor(row);
      Message<String> message =
          MessageBuilder.withPayload(row.getPayload())
              .setHeader(KafkaHeaders.TOPIC, topic)
              .setHeader(KafkaHeaders.KEY, row.getAggregateId())
              .setHeader("event-id", row.getEventId().toString())
              .setHeader("event-type", row.getEventType())
              .setHeader("aggregate-type", row.getAggregateType())
              .setHeader("aggregate-id", row.getAggregateId())
              .build();
      try {
        // Synchronous send: wait for broker ack before marking published. Async would let
        // us advance the outbox row before the broker has the message, which is exactly
        // the dual-write race we're solving. Week 8 introduces a Kafka transactional
        // producer; until then, sync send + acks=all + idempotence=true is the floor.
        kafkaTemplate.send(message).get();
      } catch (Exception e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Failed to publish outbox row " + row.getEventId() + " to " + topic, e);
      }
      row.markPublished();
      publishedCounter.increment();
    }
    LOG.debug("Published {} outbox rows from shard {}", batch.size(), shard);
    return batch.size();
  }

  String topicFor(OutboxRecord row) {
    return props.getTopicPrefix() + "." + row.getAggregateType().toLowerCase() + ".v1";
  }

  private void refreshUnpublishedGauge() {
    Long unpublished =
        em.createQuery(
                "select count(r) from OutboxRecord r where r.publishedAt is null", Long.class)
            .getSingleResult();
    meterRegistry.gauge("outbox.unpublished.count", unpublished);
  }
}
