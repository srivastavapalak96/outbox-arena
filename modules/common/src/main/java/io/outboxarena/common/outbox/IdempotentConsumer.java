package io.outboxarena.common.outbox;

import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Wraps a consumer-side handler with idempotency. Caller passes the incoming event_id and the work
 * to do. We attempt to {@code INSERT INTO processed_events (event_id, consumer_group)} first; if it
 * collides on the PK, the event has been seen before and the work is skipped silently. Both the
 * insert and the business write happen in the same transaction, so duplicate detection is atomic
 * with the side effects it gates.
 *
 * <p>This is the consumer-side defence against the duplicate that Kafka's enable.idempotence cannot
 * dedupe -- the one produced by a poller crash between Kafka commit and Postgres UPDATE (ADR-0005).
 */
@Component
public class IdempotentConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(IdempotentConsumer.class);

  private final ProcessedEventRepository processedEvents;

  public IdempotentConsumer(ProcessedEventRepository processedEvents) {
    this.processedEvents = processedEvents;
  }

  /**
   * Run {@code work} exactly once per (event_id, consumer_group). Returns true if the work actually
   * ran (first time for this event_id); false if it was a duplicate that we skipped.
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public boolean once(UUID eventId, String consumerGroup, Supplier<Void> work) {
    try {
      processedEvents.saveAndFlush(new ProcessedEvent(eventId, consumerGroup));
    } catch (DataIntegrityViolationException duplicate) {
      LOG.debug("Dropping duplicate event {} for consumer group {}", eventId, consumerGroup);
      return false;
    }
    work.get();
    return true;
  }
}
