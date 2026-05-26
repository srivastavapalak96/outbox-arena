package io.outboxarena.common.events;

/** Canonical event-type strings carried on the {@code event-type} Kafka header. */
public final class EventTypes {

  // commands (orchestrator -> participants)
  public static final String ORDER_CREATED = "OrderCreated";
  public static final String PAYMENT_REQUESTED = "PaymentRequested";
  public static final String PAYMENT_REFUND_REQUESTED = "PaymentRefundRequested";
  public static final String INVENTORY_RESERVE_REQUESTED = "InventoryReservationRequested";
  public static final String INVENTORY_RELEASE_REQUESTED = "InventoryReleaseRequested";
  public static final String SHIPPING_REQUESTED = "ShippingRequested";
  public static final String SHIPPING_CANCELLATION_REQUESTED = "ShippingCancellationRequested";

  // replies (participants -> orchestrator)
  public static final String PAYMENT_AUTHORIZED = "PaymentAuthorized";
  public static final String PAYMENT_FAILED = "PaymentFailed";
  public static final String PAYMENT_REFUNDED = "PaymentRefunded";
  public static final String INVENTORY_RESERVED = "InventoryReserved";
  public static final String INVENTORY_REJECTED = "InventoryRejected";
  public static final String INVENTORY_RELEASED = "InventoryReleased";
  public static final String SHIPMENT_DISPATCHED = "ShipmentDispatched";
  public static final String SHIPMENT_FAILED = "ShipmentFailed";

  // terminal saga signals
  public static final String ORDER_COMPLETED = "OrderCompleted";
  public static final String ORDER_CANCELLED = "OrderCancelled";

  private EventTypes() {}
}
