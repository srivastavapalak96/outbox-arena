package io.outboxarena.common;

/**
 * Marker class. The common module is a plain library -- not a Spring Boot application. Production
 * code (outbox poller, idempotency helpers, OTel context propagation, shared event DTOs) lives in
 * subpackages added in week 2 onward.
 */
public final class CommonModule {

  private CommonModule() {}
}
