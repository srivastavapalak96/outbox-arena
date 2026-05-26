package io.outboxarena.common.events;

import java.util.UUID;

/** Payment command + reply payloads. Serialised to outbox/payload JSONB. */
public final class PaymentEvents {

  private PaymentEvents() {}

  public record PaymentRequested(UUID orderUuid, long amountCents, String currency) {}

  public record PaymentAuthorized(UUID orderUuid, UUID paymentUuid, String gatewayRef) {}

  public record PaymentFailed(UUID orderUuid, String reason) {}

  public record PaymentRefundRequested(UUID orderUuid) {}

  public record PaymentRefunded(UUID orderUuid, UUID paymentUuid) {}
}
