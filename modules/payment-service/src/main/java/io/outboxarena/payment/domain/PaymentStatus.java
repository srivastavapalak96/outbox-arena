package io.outboxarena.payment.domain;

public enum PaymentStatus {
  PENDING,
  AUTHORIZED,
  CAPTURED,
  FAILED,
  REFUNDED
}
