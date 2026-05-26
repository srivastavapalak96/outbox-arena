package io.outboxarena.order.domain;

/**
 * Order lifecycle states. Implements the saga state machine: PENDING -> PAYMENT_AUTHORIZED ->
 * INVENTORY_RESERVED -> SHIPPED -> COMPLETED on the happy path; failure paths fan into the
 * COMPENSATING_* / CANCELLED branches via the orchestrator's compensation handlers.
 */
public enum OrderStatus {
  PENDING,
  PAYMENT_AUTHORIZED,
  INVENTORY_RESERVED,
  SHIPPED,
  COMPLETED,
  COMPENSATING_INVENTORY,
  COMPENSATING_PAYMENT,
  CANCELLED
}
