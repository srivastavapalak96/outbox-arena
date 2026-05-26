package io.outboxarena.common.events;

import java.util.UUID;

public final class ShippingEvents {

  private ShippingEvents() {}

  public record ShippingRequested(UUID orderUuid) {}

  public record ShipmentDispatched(UUID orderUuid, UUID shipmentUuid, String trackingNo) {}

  public record ShipmentFailed(UUID orderUuid, String reason) {}

  public record ShippingCancellationRequested(UUID orderUuid) {}
}
