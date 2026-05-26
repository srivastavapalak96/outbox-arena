package io.outboxarena.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "payments")
public class Payment {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "payment_uuid", nullable = false, unique = true, updatable = false)
  private UUID paymentUuid;

  @Column(name = "order_uuid", nullable = false, updatable = false)
  private UUID orderUuid;

  @Column(name = "amount_cents", nullable = false)
  private long amountCents;

  @Column(name = "currency", nullable = false, length = 3)
  private String currency;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 32)
  private PaymentStatus status;

  @Column(name = "gateway_ref", length = 128)
  private String gatewayRef;

  @Column(name = "idempotency_key", nullable = false, unique = true, updatable = false)
  private UUID idempotencyKey;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @Column(name = "updated_at", nullable = false)
  private OffsetDateTime updatedAt;

  protected Payment() {}

  public Payment(UUID orderUuid, long amountCents, String currency, UUID idempotencyKey) {
    this.paymentUuid = UUID.randomUUID();
    this.orderUuid = orderUuid;
    this.amountCents = amountCents;
    this.currency = currency;
    this.idempotencyKey = idempotencyKey;
    this.status = PaymentStatus.PENDING;
    OffsetDateTime now = OffsetDateTime.now();
    this.createdAt = now;
    this.updatedAt = now;
  }

  public Long getId() {
    return id;
  }

  public UUID getPaymentUuid() {
    return paymentUuid;
  }

  public UUID getOrderUuid() {
    return orderUuid;
  }

  public long getAmountCents() {
    return amountCents;
  }

  public String getCurrency() {
    return currency;
  }

  public PaymentStatus getStatus() {
    return status;
  }

  public String getGatewayRef() {
    return gatewayRef;
  }

  public UUID getIdempotencyKey() {
    return idempotencyKey;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public OffsetDateTime getUpdatedAt() {
    return updatedAt;
  }

  public void authorize(String gatewayRef) {
    this.status = PaymentStatus.AUTHORIZED;
    this.gatewayRef = gatewayRef;
    this.updatedAt = OffsetDateTime.now();
  }

  public void fail() {
    this.status = PaymentStatus.FAILED;
    this.updatedAt = OffsetDateTime.now();
  }

  public void refund() {
    this.status = PaymentStatus.REFUNDED;
    this.updatedAt = OffsetDateTime.now();
  }
}
