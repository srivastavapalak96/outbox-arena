package io.outboxarena.inventory.consume;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.outboxarena.common.events.EventTypes;
import io.outboxarena.common.events.InventoryEvents;
import io.outboxarena.common.events.Topics;
import io.outboxarena.common.outbox.IdempotentConsumer;
import io.outboxarena.common.outbox.OutboxRecord;
import io.outboxarena.common.outbox.OutboxRecordRepository;
import io.outboxarena.inventory.domain.Inventory;
import io.outboxarena.inventory.domain.InventoryKey;
import io.outboxarena.inventory.domain.InventoryRepository;
import io.outboxarena.inventory.domain.InventoryReservation;
import io.outboxarena.inventory.domain.InventoryReservationRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to commands.inventory.v1 for InventoryReservationRequested. For each line item, tries to
 * reserve stock against the (sku, seller_id) inventory row. If every line succeeds commits all the
 * InventoryReservation rows + an InventoryReserved outbox row; otherwise rolls back any partial
 * reservations and emits InventoryRejected.
 */
@Component
public class InventoryConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(InventoryConsumer.class);
  private static final int OUTBOX_SHARDS = 16;
  static final String CONSUMER_GROUP = "inventory-service";

  private final IdempotentConsumer idempotent;
  private final InventoryRepository inventory;
  private final InventoryReservationRepository reservations;
  private final OutboxRecordRepository outbox;
  private final ObjectMapper mapper;

  public InventoryConsumer(
      IdempotentConsumer idempotent,
      InventoryRepository inventory,
      InventoryReservationRepository reservations,
      OutboxRecordRepository outbox,
      ObjectMapper mapper) {
    this.idempotent = idempotent;
    this.inventory = inventory;
    this.reservations = reservations;
    this.outbox = outbox;
    this.mapper = mapper;
  }

  @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = CONSUMER_GROUP)
  public void onInventoryCommand(ConsumerRecord<String, String> record) {
    String eventType = headerOrNull(record, "event-type");
    if (!EventTypes.INVENTORY_RESERVE_REQUESTED.equals(eventType)) {
      LOG.debug("Ignoring unknown inventory command type {}", eventType);
      return;
    }
    UUID eventId = UUID.fromString(headerOrNull(record, "event-id"));
    InventoryEvents.InventoryReservationRequested req =
        readJson(record.value(), InventoryEvents.InventoryReservationRequested.class);

    idempotent.once(
        eventId,
        CONSUMER_GROUP,
        () -> {
          reserve(req);
          return null;
        });
  }

  private void reserve(InventoryEvents.InventoryReservationRequested req) {
    // Two-pass: first check every line against current stock; if any line can't be
    // satisfied, emit InventoryRejected without mutating. Only if every line passes do
    // we actually reserve. Avoids "partial reservation rolled back by exception" which
    // would couple consumer behaviour to Spring's exception-driven rollback semantics.
    List<Inventory> stockRows = new ArrayList<>();
    List<InventoryEvents.InventoryRejected.RejectedLine> rejections = new ArrayList<>();
    for (InventoryEvents.InventoryReservationRequested.Line line : req.items()) {
      Inventory stock =
          inventory.findById(new InventoryKey(line.sku(), line.sellerId())).orElse(null);
      if (stock == null || stock.getAvailable() < line.qty()) {
        rejections.add(
            new InventoryEvents.InventoryRejected.RejectedLine(
                line.sellerId(), line.sku(), line.qty(), stock == null ? 0 : stock.getAvailable()));
      }
      stockRows.add(stock);
    }

    String replyType;
    Object replyPayload;
    if (!rejections.isEmpty()) {
      replyType = EventTypes.INVENTORY_REJECTED;
      replyPayload =
          new InventoryEvents.InventoryRejected(
              req.orderUuid(), "insufficient inventory", rejections);
    } else {
      List<UUID> reservationUuids = new ArrayList<>();
      for (int i = 0; i < req.items().size(); i++) {
        InventoryEvents.InventoryReservationRequested.Line line = req.items().get(i);
        Inventory stock = stockRows.get(i);
        boolean ok = stock.reserve(line.qty());
        if (!ok) {
          throw new IllegalStateException(
              "stock vanished between dry-run and commit: " + line.sku());
        }
        InventoryReservation r =
            new InventoryReservation(req.orderUuid(), line.sku(), line.sellerId(), line.qty());
        reservations.save(r);
        reservationUuids.add(r.getReservationUuid());
      }
      replyType = EventTypes.INVENTORY_RESERVED;
      replyPayload = new InventoryEvents.InventoryReserved(req.orderUuid(), reservationUuids);
    }

    OutboxRecord reply =
        new OutboxRecord(
            UUID.randomUUID(),
            "Inventory",
            req.orderUuid().toString(),
            replyType,
            writeJson(replyPayload),
            shardKeyFor(req.orderUuid()));
    outbox.save(reply);
  }

  private static String headerOrNull(ConsumerRecord<String, String> record, String name) {
    var header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value());
  }

  private <T> T readJson(String json, Class<T> type) {
    try {
      return mapper.readValue(json, type);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to decode " + type.getSimpleName(), e);
    }
  }

  private String writeJson(Object o) {
    try {
      return mapper.writeValueAsString(o);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to encode " + o.getClass().getSimpleName(), e);
    }
  }

  static short shardKeyFor(UUID orderUuid) {
    return (short) Math.floorMod(orderUuid.hashCode(), OUTBOX_SHARDS);
  }
}
