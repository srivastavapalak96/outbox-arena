package io.outboxarena.common.events;

/**
 * Single source of truth for command-plane topic names. Saga participants reference these constants
 * rather than string literals so a typo can't accidentally publish to the wrong topic. CDC-plane
 * topics (cdc.public.*) are owned by Debezium config, not application code.
 */
public final class Topics {

  public static final String ORDER_COMMANDS = "commands.order.v1";
  public static final String PAYMENT_COMMANDS = "commands.payment.v1";
  public static final String INVENTORY_COMMANDS = "commands.inventory.v1";
  public static final String SHIPPING_COMMANDS = "commands.shipping.v1";

  /** Reply topics. Saga participants emit replies the orchestrator listens to. */
  public static final String PAYMENT_EVENTS = "events.payment.v1";

  public static final String INVENTORY_EVENTS = "events.inventory.v1";
  public static final String SHIPPING_EVENTS = "events.shipping.v1";

  private Topics() {}
}
