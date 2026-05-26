package io.outboxarena.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Saga state row, one per order. The orchestrator (week 4) updates this in the same transaction
 * that consumes each saga-reply event, so the row is the source of truth for "what step are we on"
 * -- queryable in psql for debugging.
 */
@Entity
@Table(name = "sagas")
public class Saga {

  @Id
  @Column(name = "saga_id", nullable = false, updatable = false)
  private UUID sagaId;

  @Column(name = "order_id", nullable = false)
  private Long orderId;

  @Enumerated(EnumType.STRING)
  @Column(name = "state", nullable = false, length = 32)
  private OrderStatus state;

  @Column(name = "current_step", nullable = false, length = 64)
  private String currentStep;

  @Column(name = "retries", nullable = false)
  private int retries;

  @Column(name = "last_error")
  private String lastError;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @Column(name = "updated_at", nullable = false)
  private OffsetDateTime updatedAt;

  protected Saga() {}

  public Saga(UUID sagaId, Long orderId) {
    this.sagaId = sagaId;
    this.orderId = orderId;
    this.state = OrderStatus.PENDING;
    this.currentStep = "intake";
    this.retries = 0;
    OffsetDateTime now = OffsetDateTime.now();
    this.createdAt = now;
    this.updatedAt = now;
  }

  public UUID getSagaId() {
    return sagaId;
  }

  public Long getOrderId() {
    return orderId;
  }

  public OrderStatus getState() {
    return state;
  }

  public String getCurrentStep() {
    return currentStep;
  }

  public int getRetries() {
    return retries;
  }

  public String getLastError() {
    return lastError;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public OffsetDateTime getUpdatedAt() {
    return updatedAt;
  }
}
