package io.outboxarena.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "inventory")
@IdClass(InventoryKey.class)
public class Inventory {

  @Id
  @Column(name = "sku", nullable = false, length = 64)
  private String sku;

  @Id
  @Column(name = "seller_id", nullable = false, length = 64)
  private String sellerId;

  @Column(name = "on_hand", nullable = false)
  private int onHand;

  @Column(name = "reserved", nullable = false)
  private int reserved;

  @Version
  @Column(name = "version", nullable = false)
  private long version;

  protected Inventory() {}

  public Inventory(String sku, String sellerId, int onHand) {
    if (onHand < 0) {
      throw new IllegalArgumentException("onHand must be >= 0");
    }
    this.sku = sku;
    this.sellerId = sellerId;
    this.onHand = onHand;
    this.reserved = 0;
  }

  public String getSku() {
    return sku;
  }

  public String getSellerId() {
    return sellerId;
  }

  public int getOnHand() {
    return onHand;
  }

  public int getReserved() {
    return reserved;
  }

  public int getAvailable() {
    return onHand - reserved;
  }

  public long getVersion() {
    return version;
  }

  /** Attempts to reserve {@code qty} from available stock. Returns false on insufficient. */
  public boolean reserve(int qty) {
    if (qty <= 0) {
      throw new IllegalArgumentException("qty must be > 0");
    }
    if (getAvailable() < qty) {
      return false;
    }
    this.reserved += qty;
    return true;
  }

  public void release(int qty) {
    if (qty <= 0 || qty > reserved) {
      throw new IllegalArgumentException("invalid release qty");
    }
    this.reserved -= qty;
  }

  public void commit(int qty) {
    if (qty <= 0 || qty > reserved || qty > onHand) {
      throw new IllegalArgumentException("invalid commit qty");
    }
    this.reserved -= qty;
    this.onHand -= qty;
  }
}
