package io.outboxarena.inventory.domain;

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
@Table(name = "inventory_reservations")
public class InventoryReservation {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "reservation_uuid", nullable = false, unique = true, updatable = false)
  private UUID reservationUuid;

  @Column(name = "order_uuid", nullable = false, updatable = false)
  private UUID orderUuid;

  @Column(name = "sku", nullable = false, length = 64, updatable = false)
  private String sku;

  @Column(name = "seller_id", nullable = false, length = 64, updatable = false)
  private String sellerId;

  @Column(name = "qty", nullable = false, updatable = false)
  private int qty;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 32)
  private ReservationStatus status;

  @Column(name = "expires_at", nullable = false)
  private OffsetDateTime expiresAt;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  protected InventoryReservation() {}

  public InventoryReservation(UUID orderUuid, String sku, String sellerId, int qty) {
    this.reservationUuid = UUID.randomUUID();
    this.orderUuid = orderUuid;
    this.sku = sku;
    this.sellerId = sellerId;
    this.qty = qty;
    this.status = ReservationStatus.HELD;
    this.createdAt = OffsetDateTime.now();
    this.expiresAt = this.createdAt.plusMinutes(15);
  }

  public Long getId() {
    return id;
  }

  public UUID getReservationUuid() {
    return reservationUuid;
  }

  public UUID getOrderUuid() {
    return orderUuid;
  }

  public String getSku() {
    return sku;
  }

  public String getSellerId() {
    return sellerId;
  }

  public int getQty() {
    return qty;
  }

  public ReservationStatus getStatus() {
    return status;
  }

  public OffsetDateTime getExpiresAt() {
    return expiresAt;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public void release() {
    this.status = ReservationStatus.RELEASED;
  }

  public void commit() {
    this.status = ReservationStatus.COMMITTED;
  }
}
