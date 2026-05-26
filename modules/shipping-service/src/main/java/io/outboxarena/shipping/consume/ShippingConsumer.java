package io.outboxarena.shipping.consume;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.outboxarena.common.events.EventTypes;
import io.outboxarena.common.events.ShippingEvents;
import io.outboxarena.common.events.Topics;
import io.outboxarena.common.outbox.IdempotentConsumer;
import io.outboxarena.common.outbox.OutboxRecord;
import io.outboxarena.common.outbox.OutboxRecordRepository;
import io.outboxarena.shipping.domain.Shipment;
import io.outboxarena.shipping.domain.ShipmentRepository;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to commands.shipping.v1 for ShippingRequested. Stub carrier rotation by orderUuid hash so
 * the test can predict outcomes. Writes shipments row + reply outbox row in one transaction.
 */
@Component
public class ShippingConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(ShippingConsumer.class);
  private static final int OUTBOX_SHARDS = 16;
  private static final String[] CARRIERS = {"FedEx", "Delhivery", "Bluedart"};
  static final String CONSUMER_GROUP = "shipping-service";

  private final IdempotentConsumer idempotent;
  private final ShipmentRepository shipments;
  private final OutboxRecordRepository outbox;
  private final ObjectMapper mapper;

  public ShippingConsumer(
      IdempotentConsumer idempotent,
      ShipmentRepository shipments,
      OutboxRecordRepository outbox,
      ObjectMapper mapper) {
    this.idempotent = idempotent;
    this.shipments = shipments;
    this.outbox = outbox;
    this.mapper = mapper;
  }

  @KafkaListener(topics = Topics.SHIPPING_COMMANDS, groupId = CONSUMER_GROUP)
  public void onShippingCommand(ConsumerRecord<String, String> record) {
    String eventType = headerOrNull(record, "event-type");
    if (!EventTypes.SHIPPING_REQUESTED.equals(eventType)) {
      LOG.debug("Ignoring unknown shipping command type {}", eventType);
      return;
    }
    UUID eventId = UUID.fromString(headerOrNull(record, "event-id"));
    ShippingEvents.ShippingRequested req =
        readJson(record.value(), ShippingEvents.ShippingRequested.class);

    idempotent.once(
        eventId,
        CONSUMER_GROUP,
        () -> {
          dispatch(req);
          return null;
        });
  }

  private void dispatch(ShippingEvents.ShippingRequested req) {
    String carrier = CARRIERS[Math.floorMod(req.orderUuid().hashCode(), CARRIERS.length)];
    Shipment shipment = new Shipment(req.orderUuid(), carrier);
    String trackingNo = carrier.toUpperCase() + "-" + req.orderUuid().toString().substring(0, 8);
    shipment.dispatch(trackingNo);
    shipments.save(shipment);

    ShippingEvents.ShipmentDispatched payload =
        new ShippingEvents.ShipmentDispatched(
            req.orderUuid(), shipment.getShipmentUuid(), trackingNo);
    OutboxRecord reply =
        new OutboxRecord(
            UUID.randomUUID(),
            "Shipping",
            req.orderUuid().toString(),
            EventTypes.SHIPMENT_DISPATCHED,
            writeJson(payload),
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
