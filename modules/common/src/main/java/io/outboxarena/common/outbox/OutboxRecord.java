package io.outboxarena.common.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * One row in a service's outbox table. Written inside the same transaction as the business change
 * it describes; polled and shipped to Kafka by the {@link OutboxPoller} on a scheduled tick.
 */
@Entity
@Table(name = "outbox")
public class OutboxRecord {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "event_id", nullable = false, unique = true, updatable = false)
  private UUID eventId;

  @Column(name = "aggregate_type", nullable = false, length = 64, updatable = false)
  private String aggregateType;

  @Column(name = "aggregate_id", nullable = false, length = 64, updatable = false)
  private String aggregateId;

  @Column(name = "event_type", nullable = false, length = 64, updatable = false)
  private String eventType;

  @Column(name = "payload", nullable = false, columnDefinition = "jsonb", updatable = false)
  @JdbcTypeCode(SqlTypes.JSON)
  private String payload;

  @Column(name = "headers", nullable = false, columnDefinition = "jsonb")
  @JdbcTypeCode(SqlTypes.JSON)
  private String headers = "{}";

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @Column(name = "published_at")
  private OffsetDateTime publishedAt;

  @Column(name = "shard_key", nullable = false)
  private Short shardKey;

  protected OutboxRecord() {}

  public OutboxRecord(
      UUID eventId,
      String aggregateType,
      String aggregateId,
      String eventType,
      String payload,
      short shardKey) {
    this.eventId = eventId;
    this.aggregateType = aggregateType;
    this.aggregateId = aggregateId;
    this.eventType = eventType;
    this.payload = payload;
    this.shardKey = shardKey;
    this.createdAt = OffsetDateTime.now();
  }

  public Long getId() {
    return id;
  }

  public UUID getEventId() {
    return eventId;
  }

  public String getAggregateType() {
    return aggregateType;
  }

  public String getAggregateId() {
    return aggregateId;
  }

  public String getEventType() {
    return eventType;
  }

  public String getPayload() {
    return payload;
  }

  public String getHeaders() {
    return headers;
  }

  public void setHeaders(String headers) {
    this.headers = headers;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public OffsetDateTime getPublishedAt() {
    return publishedAt;
  }

  public void markPublished() {
    this.publishedAt = OffsetDateTime.now();
  }

  public Short getShardKey() {
    return shardKey;
  }
}
