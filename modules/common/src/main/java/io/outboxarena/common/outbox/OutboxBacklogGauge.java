package io.outboxarena.common.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.transaction.annotation.Transactional;

/**
 * Single-gauge bean that exposes the count of unpublished outbox rows per saga participant.
 * Computed lazily on every Prometheus scrape -- at portfolio scale a single COUNT(*) on a
 * partial-indexed table is sub-millisecond. If this ever becomes a hot spot, replace the gauge with
 * a scheduled snapshot updated by the poller after each sweep.
 *
 * <p>The metric is the headline KPI for "is the outbox falling behind?" -- it's the first panel of
 * the Outbox Health Grafana dashboard.
 */
public class OutboxBacklogGauge {

  @PersistenceContext private EntityManager em;

  public OutboxBacklogGauge(MeterRegistry registry) {
    Gauge.builder("outbox.unpublished.count", this, OutboxBacklogGauge::countUnpublished)
        .description("Outbox rows not yet shipped to Kafka")
        .register(registry);
  }

  @Transactional(readOnly = true)
  long countUnpublished() {
    try {
      Long n =
          em.createQuery(
                  "select count(r) from OutboxRecord r where r.publishedAt is null", Long.class)
              .getSingleResult();
      return n == null ? 0L : n;
    } catch (Exception e) {
      return 0L;
    }
  }
}
