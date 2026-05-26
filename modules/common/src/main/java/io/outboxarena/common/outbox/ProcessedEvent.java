package io.outboxarena.common.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Consumer-side idempotency marker. Insert one row per (event_id, consumer_group); a duplicate-key
 * violation means "already processed -- drop the event." Must be written in the same transaction as
 * the downstream business write.
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

  @Id
  @Column(name = "event_id", nullable = false, updatable = false)
  private UUID eventId;

  @Column(name = "consumer_group", nullable = false, length = 64, updatable = false)
  private String consumerGroup;

  @Column(name = "processed_at", nullable = false, updatable = false)
  private OffsetDateTime processedAt;

  protected ProcessedEvent() {}

  public ProcessedEvent(UUID eventId, String consumerGroup) {
    this.eventId = eventId;
    this.consumerGroup = consumerGroup;
    this.processedAt = OffsetDateTime.now();
  }

  public UUID getEventId() {
    return eventId;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public OffsetDateTime getProcessedAt() {
    return processedAt;
  }
}
