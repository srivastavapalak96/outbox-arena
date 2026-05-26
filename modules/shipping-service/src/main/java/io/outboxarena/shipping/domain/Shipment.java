package io.outboxarena.shipping.domain;

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
@Table(name = "shipments")
public class Shipment {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "shipment_uuid", nullable = false, unique = true, updatable = false)
  private UUID shipmentUuid;

  @Column(name = "order_uuid", nullable = false, unique = true, updatable = false)
  private UUID orderUuid;

  @Column(name = "carrier", nullable = false, length = 32)
  private String carrier;

  @Column(name = "tracking_no", length = 64)
  private String trackingNo;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 32)
  private ShipmentStatus status;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @Column(name = "updated_at", nullable = false)
  private OffsetDateTime updatedAt;

  protected Shipment() {}

  public Shipment(UUID orderUuid, String carrier) {
    this.shipmentUuid = UUID.randomUUID();
    this.orderUuid = orderUuid;
    this.carrier = carrier;
    this.status = ShipmentStatus.REQUESTED;
    OffsetDateTime now = OffsetDateTime.now();
    this.createdAt = now;
    this.updatedAt = now;
  }

  public Long getId() {
    return id;
  }

  public UUID getShipmentUuid() {
    return shipmentUuid;
  }

  public UUID getOrderUuid() {
    return orderUuid;
  }

  public String getCarrier() {
    return carrier;
  }

  public String getTrackingNo() {
    return trackingNo;
  }

  public ShipmentStatus getStatus() {
    return status;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public OffsetDateTime getUpdatedAt() {
    return updatedAt;
  }

  public void dispatch(String trackingNo) {
    this.status = ShipmentStatus.DISPATCHED;
    this.trackingNo = trackingNo;
    this.updatedAt = OffsetDateTime.now();
  }

  public void fail() {
    this.status = ShipmentStatus.FAILED;
    this.updatedAt = OffsetDateTime.now();
  }
}
