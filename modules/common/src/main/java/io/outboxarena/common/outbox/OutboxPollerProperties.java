package io.outboxarena.common.outbox;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration knobs for the outbox poller. Bound from {@code outbox-arena.outbox.*} in
 * application.yml. Defaults are chosen to keep a single-instance local dev loop responsive
 * (sub-second median publish lag) without hammering the DB at idle.
 */
@ConfigurationProperties(prefix = "outbox-arena.outbox")
public class OutboxPollerProperties {

  /** Master switch. Set to false in tests that want to drive the poller manually. */
  private boolean enabled = true;

  /** Poll cadence -- how often the scheduled bean runs per shard sweep. */
  private Duration pollInterval = Duration.ofMillis(200);

  /** Rows pulled per shard sweep. Higher = larger batches, longer per-iteration latency. */
  private int batchSize = 100;

  /** Shards this instance is responsible for. Default: all 16. Phase 3 wires lease-table. */
  private List<Integer> shards = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

  /** Kafka topic prefix; final topic name = "{prefix}.{aggregate-type-lowercase}.v1". */
  private String topicPrefix = "commands";

  /** Producer transactional id base; one per service replica + shard. */
  private String transactionalIdPrefix = "outbox-tx";

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Duration getPollInterval() {
    return pollInterval;
  }

  public void setPollInterval(Duration pollInterval) {
    this.pollInterval = pollInterval;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public List<Integer> getShards() {
    return shards;
  }

  public void setShards(List<Integer> shards) {
    this.shards = shards;
  }

  public String getTopicPrefix() {
    return topicPrefix;
  }

  public void setTopicPrefix(String topicPrefix) {
    this.topicPrefix = topicPrefix;
  }

  public String getTransactionalIdPrefix() {
    return transactionalIdPrefix;
  }

  public void setTransactionalIdPrefix(String transactionalIdPrefix) {
    this.transactionalIdPrefix = transactionalIdPrefix;
  }
}
