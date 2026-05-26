package io.outboxarena.payment.consume;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.outboxarena.common.events.EventTypes;
import io.outboxarena.common.events.PaymentEvents;
import io.outboxarena.common.events.Topics;
import io.outboxarena.common.outbox.IdempotentConsumer;
import io.outboxarena.common.outbox.OutboxRecord;
import io.outboxarena.common.outbox.OutboxRecordRepository;
import io.outboxarena.payment.domain.Payment;
import io.outboxarena.payment.domain.PaymentRepository;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to commands.payment.v1 for PaymentRequested. Writes the payment row, the reply outbox
 * row, and the processed_events row inside one transaction.
 *
 * <p>The stub "gateway": amounts &le; 100,000 cents authorise; above fails. Deterministic by design
 * so the week-5 compensation tests can drive the rejection path reliably.
 */
@Component
public class PaymentConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(PaymentConsumer.class);
  private static final long AUTHORISE_CEILING_CENTS = 100_000L;
  private static final int OUTBOX_SHARDS = 16;
  static final String CONSUMER_GROUP = "payment-service";

  private final IdempotentConsumer idempotent;
  private final PaymentRepository payments;
  private final OutboxRecordRepository outbox;
  private final ObjectMapper mapper;

  public PaymentConsumer(
      IdempotentConsumer idempotent,
      PaymentRepository payments,
      OutboxRecordRepository outbox,
      ObjectMapper mapper) {
    this.idempotent = idempotent;
    this.payments = payments;
    this.outbox = outbox;
    this.mapper = mapper;
  }

  @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = CONSUMER_GROUP)
  public void onPaymentCommand(ConsumerRecord<String, String> record) {
    String eventType = headerOrNull(record, "event-type");
    if (eventType == null) {
      return;
    }
    UUID eventId = UUID.fromString(headerOrNull(record, "event-id"));

    if (EventTypes.PAYMENT_REQUESTED.equals(eventType)) {
      PaymentEvents.PaymentRequested req =
          readJson(record.value(), PaymentEvents.PaymentRequested.class);
      idempotent.once(
          eventId,
          CONSUMER_GROUP,
          () -> {
            authorise(req, eventId);
            return null;
          });
    } else if (EventTypes.PAYMENT_REFUND_REQUESTED.equals(eventType)) {
      PaymentEvents.PaymentRefundRequested req =
          readJson(record.value(), PaymentEvents.PaymentRefundRequested.class);
      idempotent.once(
          eventId,
          CONSUMER_GROUP,
          () -> {
            refund(req);
            return null;
          });
    } else {
      LOG.debug("Ignoring unknown payment command type {}", eventType);
    }
  }

  private void authorise(PaymentEvents.PaymentRequested req, UUID incomingEventId) {
    Payment payment =
        new Payment(req.orderUuid(), req.amountCents(), req.currency(), incomingEventId);

    String replyType;
    Object replyPayload;
    if (req.amountCents() <= AUTHORISE_CEILING_CENTS) {
      payment.authorize("stub-gw-" + UUID.randomUUID());
      replyType = EventTypes.PAYMENT_AUTHORIZED;
      replyPayload =
          new PaymentEvents.PaymentAuthorized(
              req.orderUuid(), payment.getPaymentUuid(), payment.getGatewayRef());
    } else {
      payment.fail();
      replyType = EventTypes.PAYMENT_FAILED;
      replyPayload =
          new PaymentEvents.PaymentFailed(req.orderUuid(), "amount exceeds authorise ceiling");
    }
    payments.save(payment);

    OutboxRecord reply =
        new OutboxRecord(
            UUID.randomUUID(),
            "Payment",
            req.orderUuid().toString(),
            replyType,
            writeJson(replyPayload),
            shardKeyFor(req.orderUuid()));
    outbox.save(reply);
  }

  private void refund(PaymentEvents.PaymentRefundRequested req) {
    Payment payment =
        payments
            .findByOrderUuid(req.orderUuid())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Refund requested for unknown order " + req.orderUuid()));
    // Idempotent at the row level too: an already-refunded payment short-circuits.
    if (payment.getStatus() != io.outboxarena.payment.domain.PaymentStatus.REFUNDED) {
      payment.refund();
    }

    PaymentEvents.PaymentRefunded reply =
        new PaymentEvents.PaymentRefunded(req.orderUuid(), payment.getPaymentUuid());
    OutboxRecord row =
        new OutboxRecord(
            UUID.randomUUID(),
            "Payment",
            req.orderUuid().toString(),
            EventTypes.PAYMENT_REFUNDED,
            writeJson(reply),
            shardKeyFor(req.orderUuid()));
    outbox.save(row);
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

  // Exposed for tests
  static Map<String, Object> headersFor(UUID orderUuid) {
    Map<String, Object> h = new LinkedHashMap<>();
    h.put("aggregate-type", "Payment");
    h.put("aggregate-id", orderUuid.toString());
    return h;
  }
}
