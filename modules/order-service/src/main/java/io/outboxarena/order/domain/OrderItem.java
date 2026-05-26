package io.outboxarena.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "order_items")
public class OrderItem {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @ManyToOne
  @JoinColumn(name = "order_id", nullable = false)
  private Order order;

  @Column(name = "seller_id", nullable = false, length = 64)
  private String sellerId;

  @Column(name = "sku", nullable = false, length = 64)
  private String sku;

  @Column(name = "qty", nullable = false)
  private int qty;

  @Column(name = "unit_price_cents", nullable = false)
  private long unitPriceCents;

  protected OrderItem() {}

  public OrderItem(String sellerId, String sku, int qty, long unitPriceCents) {
    if (qty <= 0) {
      throw new IllegalArgumentException("qty must be > 0");
    }
    if (unitPriceCents < 0) {
      throw new IllegalArgumentException("unitPriceCents must be >= 0");
    }
    this.sellerId = sellerId;
    this.sku = sku;
    this.qty = qty;
    this.unitPriceCents = unitPriceCents;
  }

  void attachTo(Order order) {
    this.order = order;
  }

  public long lineTotalCents() {
    return (long) qty * unitPriceCents;
  }

  public Long getId() {
    return id;
  }

  public Order getOrder() {
    return order;
  }

  public String getSellerId() {
    return sellerId;
  }

  public String getSku() {
    return sku;
  }

  public int getQty() {
    return qty;
  }

  public long getUnitPriceCents() {
    return unitPriceCents;
  }
}
