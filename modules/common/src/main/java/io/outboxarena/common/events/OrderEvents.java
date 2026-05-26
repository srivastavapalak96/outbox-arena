package io.outboxarena.common.events;

import java.util.UUID;

public final class OrderEvents {

  private OrderEvents() {}

  public record OrderCompleted(UUID orderUuid, UUID paymentUuid, UUID shipmentUuid) {}

  public record OrderCancelled(UUID orderUuid, String reason) {}
}
