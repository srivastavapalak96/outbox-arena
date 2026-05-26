package io.outboxarena.order.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "order_uuid", nullable = false, unique = true, updatable = false)
  private UUID orderUuid;

  @Column(name = "buyer_id", nullable = false, length = 64)
  private String buyerId;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 32)
  private OrderStatus status;

  @Column(name = "total_amount_cents", nullable = false)
  private long totalAmountCents;

  @Column(name = "currency", nullable = false, length = 3)
  private String currency;

  @Column(name = "created_at", nullable = false, updatable = false)
  private OffsetDateTime createdAt;

  @Column(name = "updated_at", nullable = false)
  private OffsetDateTime updatedAt;

  @Version
  @Column(name = "version", nullable = false)
  private long version;

  @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
  private List<OrderItem> items = new ArrayList<>();

  protected Order() {}

  public Order(UUID orderUuid, String buyerId, String currency) {
    this.orderUuid = orderUuid;
    this.buyerId = buyerId;
    this.currency = currency;
    this.status = OrderStatus.PENDING;
    this.totalAmountCents = 0;
    OffsetDateTime now = OffsetDateTime.now();
    this.createdAt = now;
    this.updatedAt = now;
  }

  public void addItem(OrderItem item) {
    items.add(item);
    item.attachTo(this);
    this.totalAmountCents += item.lineTotalCents();
  }

  public Long getId() {
    return id;
  }

  public UUID getOrderUuid() {
    return orderUuid;
  }

  public String getBuyerId() {
    return buyerId;
  }

  public OrderStatus getStatus() {
    return status;
  }

  public long getTotalAmountCents() {
    return totalAmountCents;
  }

  public String getCurrency() {
    return currency;
  }

  public OffsetDateTime getCreatedAt() {
    return createdAt;
  }

  public OffsetDateTime getUpdatedAt() {
    return updatedAt;
  }

  public long getVersion() {
    return version;
  }

  public List<OrderItem> getItems() {
    return Collections.unmodifiableList(items);
  }

  public void transitionTo(OrderStatus next) {
    this.status = next;
    this.updatedAt = OffsetDateTime.now();
  }
}
