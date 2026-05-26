package io.outboxarena.projection.consume;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.outboxarena.projection.domain.OrderView;
import io.outboxarena.projection.domain.OrderViewRepository;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Pure CDC consumer. Reads Debezium change events from {@code cdc.public.orders} and upserts the
 * {@link OrderView} read model. This service has no outbox and no business logic -- it materialises
 * derived state from the source-of-truth tables (see ADR-0002).
 *
 * <p>Debezium's {@code ExtractNewRecordState} SMT is configured upstream, so the message value is
 * the flattened post-image (or tombstone-null for deletes). We parse the columns we care about and
 * let {@link OrderView#applyCdc} do the write.
 */
@Component
public class OrdersCdcConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(OrdersCdcConsumer.class);
  static final String CONSUMER_GROUP = "projection-service";

  private final OrderViewRepository views;
  private final ObjectMapper mapper;

  public OrdersCdcConsumer(OrderViewRepository views, ObjectMapper mapper) {
    this.views = views;
    this.mapper = mapper;
  }

  @KafkaListener(topics = "cdc.public.orders", groupId = CONSUMER_GROUP)
  @Transactional
  public void onOrdersChange(ConsumerRecord<String, String> record) {
    if (record.value() == null) {
      // Debezium tombstone for deletes -- we don't model deletes in the read view.
      return;
    }
    try {
      JsonNode row = mapper.readTree(record.value());
      if ("true".equals(textOrNull(row, "__deleted"))) {
        return;
      }
      String orderUuidStr = textOrNull(row, "order_uuid");
      if (orderUuidStr == null) {
        LOG.debug("Skipping CDC record without order_uuid: {}", record.value());
        return;
      }
      UUID orderUuid = UUID.fromString(orderUuidStr);
      String buyerId = textOrNull(row, "buyer_id");
      String status = textOrNull(row, "status");
      long totalAmountCents = longOrZero(row, "total_amount_cents");
      String currency = textOrNull(row, "currency");

      OrderView view =
          views
              .findById(orderUuid)
              .orElseGet(
                  () ->
                      new OrderView(
                          orderUuid,
                          buyerId,
                          status,
                          totalAmountCents,
                          currency,
                          OffsetDateTime.now()));
      view.applyCdc(buyerId, status, totalAmountCents, currency, "lsn-not-extracted-yet");
      views.save(view);
    } catch (Exception e) {
      LOG.warn("Failed to apply CDC event for orders: {}", e.toString());
      // Don't rethrow -- let the consumer commit and move on. Week-9 chaos suite will
      // exercise the DLQ path when this is non-trivial.
    }
  }

  private static String textOrNull(JsonNode row, String field) {
    JsonNode n = row.get(field);
    return (n == null || n.isNull()) ? null : n.asText();
  }

  private static long longOrZero(JsonNode row, String field) {
    JsonNode n = row.get(field);
    return (n == null || n.isNull()) ? 0L : n.asLong();
  }
}
