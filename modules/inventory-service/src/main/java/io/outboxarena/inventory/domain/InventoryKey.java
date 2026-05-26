package io.outboxarena.inventory.domain;

import java.io.Serializable;
import java.util.Objects;

/** Composite key (sku, seller_id) -- the same SKU can be offered by multiple sellers. */
public class InventoryKey implements Serializable {

  private String sku;
  private String sellerId;

  public InventoryKey() {}

  public InventoryKey(String sku, String sellerId) {
    this.sku = sku;
    this.sellerId = sellerId;
  }

  public String getSku() {
    return sku;
  }

  public String getSellerId() {
    return sellerId;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof InventoryKey that)) {
      return false;
    }
    return Objects.equals(sku, that.sku) && Objects.equals(sellerId, that.sellerId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sku, sellerId);
  }
}
