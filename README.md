# outbox-arena

> Multi-seller marketplace order-fulfillment backbone demonstrating the **Transactional Outbox**
> pattern and **Debezium CDC** as the two solutions to the **dual-write problem**, with
> reproducible chaos tests and a 1000 orders/sec Docker Compose demo.

The framing this project comes from: at scale (5-7K TPS on payment integrations), naive
write-then-publish patterns silently drop a small but non-zero fraction of events whenever
the broker, the network, or the service itself blinks. This repository is a working
production-shape system that demonstrates the patterns that eliminate that failure mode,
plus the chaos tests that turn architectural claims into measurements.

## Status

Pre-1.0 -- under active development. See [`docs/status.md`](docs/status.md) for what is
SHIPPED, PARTIAL, and PLANNED, and the canonical plan at
`.claude/plans/can-you-make-a-witty-harbor.md` (off-repo) for the 10-week roadmap.

## Headline measurements

To be populated in week 9 once the load + chaos suites complete. The structure of the table
is fixed up front so the deliverable is unambiguous:

| Scenario | Events sent | Events received | Duplicates | Loss rate |
|----------|-------------|-----------------|------------|-----------|
| Happy path, 1000/s x 5min       | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Broker kill mid-publish         | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| SIGKILL after DB commit         | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Multi-poller race (12 inst)     | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Consumer rebalance mid-batch    | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

## Architecture (intent)

```
            POST /orders                 commands.*  (Outbox-published)
   client ────────────────▶ order-service ──────────▶ payment-service ──┐
                                  │                                      │
                                  │                                      ▼
                                  │                              inventory-service
                                  │                                      │
                                  │                                      ▼
                                  └──────────────────────────────▶ shipping-service
                                                                         │
                                                  OrderCompleted/Cancelled
                                                                         ▼
                                                                  notifier-sink (stdout)

   Postgres WAL ─Debezium─▶ cdc.public.*  ──────────▶  projection-service ──▶ order_views

   Command plane: Outbox -> Kafka -> saga participants.   Owned by application code.
   Data plane:    WAL -> Debezium -> Kafka -> projector.  Owned by infrastructure.
   The boundary is the project's conceptual spine -- see ADR-0002.
```

## The dual-write problem in 4 lines

```java
// What naive code does -- and what this repo proves is broken:
transaction.begin();
orderRepository.save(order);              // (1) DB write
transaction.commit();                     // (2) commit
kafkaTemplate.send("orders.v1", event);   // (3) publish     <-- between (2) and (3), anything can fail
```

If the broker is unreachable between steps (2) and (3), the DB has the order, the world
does not. If we swap (2) and (3), we publish before commit and the world hears about an
order that may never exist. The dual-write problem is that these two systems do not share
a transaction. The fix is to make them share one, by writing the event to a database
table -- the Outbox -- inside the same business transaction, and shipping it from there.

## Stack

| Layer | Choice | Why |
|-------|--------|-----|
| Language / framework | Java 17 + Spring Boot 3 | LTS, ubiquitous in the JVM systems space |
| Persistence | PostgreSQL 16 | `pgoutput` for Debezium, `SKIP LOCKED` for poller, JSONB |
| Broker | Apache Kafka 3.7+ (KRaft) | Reference broker for Debezium; no ZooKeeper |
| CDC | Debezium 2.x Postgres connector | Tails the WAL via `pgoutput` |
| Observability | Prometheus + Grafana + OpenTelemetry + Jaeger | Stack you can run on any laptop |
| Local infra | Docker Compose | One command; matches the production shape |
| K8s | kustomize + Strimzi | Declarative; operator manages KRaft + Connect |

## Run it

```bash
# 1. Bring up the infrastructure (Postgres, Kafka, Connect, Prom, Grafana, Jaeger).
make up

# 2. Verify the stack is healthy.
make health-infra

# 3. Boot the application services (each in its own terminal, or via gradle bootRun).
./gradlew :modules:order-service:bootRun       # localhost:8081
./gradlew :modules:payment-service:bootRun     # localhost:8082
./gradlew :modules:inventory-service:bootRun   # localhost:8083
./gradlew :modules:shipping-service:bootRun    # localhost:8084
./gradlew :modules:projection-service:bootRun  # localhost:8085
./gradlew :modules:notifier-sink:bootRun       # localhost:8086

# 4. Confirm all 6 services are green.
make health
```

## Repo layout

```
modules/                  Gradle multi-module sources (common + 6 services)
  common/                 Outbox poller, idempotency, OTel context -- shared lib
  order-service/          Saga orchestrator + HTTP intake
  payment-service/        Authorize/capture/refund against stub gateway
  inventory-service/      Multi-seller stock reserve / release / commit
  shipping-service/       Book / cancel shipments against stub carrier
  projection-service/     Pure CDC consumer -- read-side projections only
  notifier-sink/          Terminal-state stdout sink (stub for email/SMS)

infra/                    docker-compose.yml + Postgres / Prom / Grafana config
k8s/                      kustomize base + overlays
chaos/scenarios/          JUnit chaos tests (broker kill, multi-poller race, ...)
load/scripts/             k6 load scripts + post-run audit SQL
docs/adr/                 Architecture Decision Records
buildSrc/                 Shared Gradle conventions
```

## ADRs

- `0001-record-architecture-decisions.md` -- meta
- `0002-outbox-and-cdc-as-distinct-planes.md` -- the conceptual spine
- `0003-postgresql-over-mysql.md`
- `0004-orchestration-saga-over-choreography.md`
- `0005-sharded-outbox-poller-with-skip-locked.md`
- `0006-postgres-processed_events-over-redis-setnx.md`
- `0007-no-debezium-outbox-smt.md`

## License

Apache 2.0 -- see `LICENSE`.
