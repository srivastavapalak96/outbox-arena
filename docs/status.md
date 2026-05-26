# Status

Last updated: post-week-9 regression sweep (7/7 chaos green + `./gradlew check` green).

**Verification at this commit:**
- `./gradlew check` -- GREEN (all unit + integration tests, Spotless, every module)
- `./gradlew build` -- GREEN (all bootJars produced)
- `chaos/scenarios/run-all.sh` -- 7/7 PASS in 275s total wall-clock
- All 7 ADRs Accepted and linked from README

| Area | Status |
|------|--------|
| Gradle multi-module skeleton | SHIPPED, `./gradlew build` green |
| 6 Spring Boot service modules | SHIPPED, all boot + healthy |
| Docker Compose: Postgres 16 (`wal_level=logical`) + Kafka 3.7 KRaft + Connect + Prom + Grafana + Jaeger | SHIPPED |
| Docker Compose chaos overlay: Toxiproxy in front of Kafka | SHIPPED |
| Flyway migrations for every saga participant + projection | SHIPPED |
| JPA entities for order/item/saga/payment/inventory/reservation/shipment/order_view | SHIPPED |
| OutboxRecord + ProcessedEvent shared in modules/common | SHIPPED |
| order-service POST /orders writes orders+items+saga+outbox atomically | SHIPPED |
| Outbox poller (sharded, `SKIP LOCKED`, sync Kafka send) | SHIPPED |
| OutboxPollerScheduler (split-bean pattern, no self-invocation gotcha) | SHIPPED |
| Shared event DTOs (Payment/Inventory/Shipping/Order) + Topic constants | SHIPPED |
| IdempotentConsumer (processed_events PK dedup) | SHIPPED |
| Saga happy path: order -> payment -> inventory -> shipping -> COMPLETED | SHIPPED, 5s end-to-end |
| Saga compensation: PaymentFailed -> CANCELLED | SHIPPED, 2s scenario green |
| Saga compensation: InventoryRejected -> Refund -> CANCELLED | SHIPPED, 6s scenario green |
| Saga compensation: ShipmentFailed -> Release -> Refund -> CANCELLED | SHIPPED, ~6s scenario green |
| Debezium connector + projection-service materialises order_views via CDC | SHIPPED, 3s end-to-end |
| Saga + outbox metrics (Micrometer / Prometheus) | SHIPPED |
| Grafana dashboards (saga-health + outbox-health) | SHIPPED, auto-provisioned |
| Chaos: broker kill mid-publish via Toxiproxy | SHIPPED, 100 events sent through 5s outage, 0 lost |
| Chaos: multi-poller race -- 4 instances on same shards | SHIPPED, 80 orders -> 80 unique events, 0 dup publishes |
| Chaos: SIGKILL order-service mid-publish | SHIPPED, 15 unpublished outbox rows survived kill, 0 lost |
| Chaos: consumer rebalance | SHIPPED, 2-instance group survived kill mid-flight, 0 duplicates |
| Chaos: regression sweep runner (`run-all.sh`) | SHIPPED, 9/9 scenarios |
| ADRs 0001-0007 (record-decisions; outbox/CDC; postgres-over-mysql; orchestration; SKIP LOCKED; processed_events; no-debezium-SMT) | SHIPPED, all 7 Accepted |
| K8s manifests (kustomize, Strimzi for Kafka, HPA on order-service) | SHIPPED, `kubectl kustomize` renders clean |
| OpenTelemetry tracing | DEFERRED (Grafana saga-health dashboard covers the same operational questions) |
| k6 1000 orders/sec load + producer-vs-consumer audit | PLANNED (post-release polish) |
| Custom-metric HPA via Prometheus adapter | PLANNED (post-release polish) |
| k6 load test + producer-vs-consumer audit @ 1000 orders/sec | PLANNED (week 9) |
| K8s manifests (kustomize + Strimzi) | PLANNED (week 10) |
| ADRs 0003-0007 | PLANNED (week 10) |

Treat anything not SHIPPED as not-yet-real.
