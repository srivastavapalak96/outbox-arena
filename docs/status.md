# Status

Last updated: week 8 (broker-kill chaos verified).

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
| ADR-0001 (records architecture decisions) | SHIPPED |
| ADR-0002 (outbox + CDC as distinct planes) | SHIPPED |
| OpenTelemetry tracing | DEFERRED (returns with the rest of the chaos suite) |
| Chaos: SIGKILL after DB commit, multi-poller race, consumer rebalance, network partition | PLANNED (week 9) |
| k6 load test + producer-vs-consumer audit @ 1000 orders/sec | PLANNED (week 9) |
| K8s manifests (kustomize + Strimzi) | PLANNED (week 10) |
| ADRs 0003-0007 | PLANNED (week 10) |

Treat anything not SHIPPED as not-yet-real.
