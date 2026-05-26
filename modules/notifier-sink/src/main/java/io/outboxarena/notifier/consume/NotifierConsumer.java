package io.outboxarena.notifier.consume;

import io.outboxarena.common.events.EventTypes;
import io.outboxarena.common.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Terminal-state sink. Stub for email/SMS fanout: just writes a line to stdout when an order is
 * completed or cancelled. No DB, no outbox -- this service is a fire-and-forget downstream
 * consumer.
 */
@Component
public class NotifierConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(NotifierConsumer.class);

  @KafkaListener(topics = Topics.ORDER_COMMANDS, groupId = "notifier-sink")
  public void onOrderEvent(ConsumerRecord<String, String> record) {
    String eventType = headerOrNull(record, "event-type");
    if (eventType == null) {
      return;
    }
    if (EventTypes.ORDER_COMPLETED.equals(eventType)) {
      LOG.info("notifier: order completed -- payload {}", record.value());
    } else if (EventTypes.ORDER_CANCELLED.equals(eventType)) {
      LOG.info("notifier: order cancelled -- payload {}", record.value());
    }
  }

  private static String headerOrNull(ConsumerRecord<String, String> record, String name) {
    var header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value());
  }
}
