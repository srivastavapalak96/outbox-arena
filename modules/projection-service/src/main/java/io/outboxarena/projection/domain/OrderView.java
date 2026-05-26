package io.outboxarena.projection.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Denormalised order projection. Built by consuming Debezium CDC events from orders, payments, and
 * shipments (week 6 wires up the consumer). This service never writes to its own database through
 * application code -- all writes come from the CDC stream.
 */
@Entity
@Table(name = "order_views")
public class OrderView {

  @Id
  @Column(name = "order_uuid", nullable = false, updatable = false)
  private UUID orderUuid;

  @Column(name = "buyer_id", nullable = false, length = 64)
  private String buyerId;

  @Column(name = "status", nullable = false, length = 32)
  private String status;

  @Column(name = "total_amount_cents", nullable = false)
  private long totalAmountCents;

  @Column(name = "currency", nullable = false, length = 3)
  private String currency;

  @Column(name = "payment_status", length = 32)
  private String paymentStatus;

  @Column(name = "shipment_status", length = 32)
  private String shipmentStatus;

  @Column(name = "tracking_no", length = 64)
  private String trackingNo;

  @Column(name = "last_updated_at", nullable = false)
  private OffsetDateTime lastUpdatedAt;

  @Column(name = "source_lsn", length = 64)
  private String sourceLsn;

  protected OrderView() {}

  public UUID getOrderUuid() {
    return orderUuid;
  }

  public String getBuyerId() {
    return buyerId;
  }

  public String getStatus() {
    return status;
  }

  public long getTotalAmountCents() {
    return totalAmountCents;
  }

  public String getCurrency() {
    return currency;
  }

  public String getPaymentStatus() {
    return paymentStatus;
  }

  public String getShipmentStatus() {
    return shipmentStatus;
  }

  public String getTrackingNo() {
    return trackingNo;
  }

  public OffsetDateTime getLastUpdatedAt() {
    return lastUpdatedAt;
  }

  public String getSourceLsn() {
    return sourceLsn;
  }
}
