package io.outboxarena.common.events;

import java.util.List;
import java.util.UUID;

public final class InventoryEvents {

  private InventoryEvents() {}

  public record InventoryReservationRequested(UUID orderUuid, List<Line> items) {

    public record Line(String sellerId, String sku, int qty) {}
  }

  public record InventoryReserved(UUID orderUuid, List<UUID> reservationUuids) {}

  public record InventoryRejected(UUID orderUuid, String reason, List<RejectedLine> rejected) {

    public record RejectedLine(String sellerId, String sku, int requested, int available) {}
  }

  public record InventoryReleaseRequested(UUID orderUuid) {}

  public record InventoryReleased(UUID orderUuid) {}
}
