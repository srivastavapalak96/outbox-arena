package io.outboxarena.common.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * The {@code @Scheduled} tick lives on this bean -- not on {@link OutboxPoller} -- so the call to
 * {@code poller.sweepShard()} goes through Spring's transactional proxy. If {@code @Scheduled} were
 * on OutboxPoller, the self-invocation from {@code sweep()} to {@code sweepShard()} would bypass
 * the proxy and lose the {@code @Transactional} semantics (the classic Spring gotcha). Splitting
 * into two beans is the standard fix.
 */
public class OutboxPollerScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(OutboxPollerScheduler.class);

  private final OutboxPoller poller;
  private final OutboxPollerProperties props;
  private final Counter failureCounter;
  private final Timer publishTimer;

  public OutboxPollerScheduler(
      OutboxPoller poller, OutboxPollerProperties props, MeterRegistry meterRegistry) {
    this.poller = poller;
    this.props = props;
    this.failureCounter =
        Counter.builder("outbox.poller.failures")
            .description("Outbox poller sweep failures (will retry on the next tick)")
            .register(meterRegistry);
    this.publishTimer =
        Timer.builder("outbox.poller.sweep.duration")
            .description("Time spent sweeping all owned shards in one tick")
            .register(meterRegistry);
  }

  @Scheduled(fixedDelayString = "${outbox-arena.outbox.poll-interval:PT0.2S}")
  public void tick() {
    if (!props.isEnabled()) {
      return;
    }
    publishTimer.record(
        () -> {
          for (Integer shard : props.getShards()) {
            try {
              poller.sweepShard(shard);
            } catch (Exception e) {
              LOG.warn("Outbox sweep failed for shard {}: {}", shard, e.toString());
              failureCounter.increment();
            }
          }
        });
  }
}
