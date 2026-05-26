package io.outboxarena.order.domain;

/**
 * Order lifecycle states. Mirrors the saga state machine sketched in the plan: PENDING ->
 * PAYMENT_AUTHORIZED -> INVENTORY_RESERVED -> SHIPPED -> COMPLETED on the happy path; failure paths
 * fan into the COMPENSATING_* / CANCELLED branches starting week 5.
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
