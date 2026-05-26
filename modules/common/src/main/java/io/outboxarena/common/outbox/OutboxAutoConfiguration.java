package io.outboxarena.common.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Brings the outbox poller into any service that imports modules/common. Wiring is manual (rather
 * than {@code @Component} on OutboxPoller) so {@code outbox-arena.outbox.enabled=false} really
 * removes every bean -- otherwise component-scan still picks the poller up and autowire fails for
 * {@code OutboxPollerProperties}.
 */
@Configuration
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnProperty(
    prefix = "outbox-arena.outbox",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@EnableConfigurationProperties(OutboxPollerProperties.class)
@EnableScheduling
public class OutboxAutoConfiguration {

  @Bean
  public OutboxPoller outboxPoller(
      OutboxPollerProperties props,
      KafkaTemplate<String, String> kafkaTemplate,
      MeterRegistry meterRegistry) {
    return new OutboxPoller(props, kafkaTemplate, meterRegistry);
  }

  @Bean
  public OutboxPollerScheduler outboxPollerScheduler(
      OutboxPoller poller, OutboxPollerProperties props, MeterRegistry meterRegistry) {
    return new OutboxPollerScheduler(poller, props, meterRegistry);
  }

  @Bean
  public IdempotentConsumer idempotentConsumer(ProcessedEventRepository processedEvents) {
    return new IdempotentConsumer(processedEvents);
  }
}
