# Status

Last updated: week 5.

| Area | Status |
|------|--------|
| Gradle multi-module skeleton | SHIPPED, `./gradlew build` green |
| 6 Spring Boot service modules (boot + actuator only) | SHIPPED, order-service + payment-service verified locally |
| Docker Compose: Postgres 16 with `wal_level=logical` | SHIPPED, healthy |
| Docker Compose: Kafka 3.7 KRaft, single broker | SHIPPED, healthy |
| Docker Compose: Kafka Connect with Debezium 2.7.3 | SHIPPED, healthy |
| Docker Compose: Prometheus + Grafana + Jaeger | SHIPPED, all reachable |
| Flyway migrations for all saga participants + projection | SHIPPED |
| JPA entities for order/item/saga/payment/inventory/reservation/shipment/order_view | SHIPPED |
| OutboxRecord + ProcessedEvent shared in modules/common | SHIPPED |
| order-service POST /orders writes orders+items+saga+outbox atomically | SHIPPED, 3 tests green |
| ADR-0001 (records architecture decisions) | SHIPPED |
| ADR-0002 (outbox + CDC as distinct planes) | SHIPPED |
| Outbox poller (sharded, SKIP LOCKED, sync Kafka send) | SHIPPED, end-to-end test green |
| OutboxPollerScheduler (split-bean pattern, no self-invocation gotcha) | SHIPPED |
| Shared event DTOs (Payment/Inventory/Shipping/Order) | SHIPPED |
| IdempotentConsumer (processed_events PK dedup) | SHIPPED |
| Saga happy path: order -> payment -> inventory -> shipping -> COMPLETED | SHIPPED, 5s end-to-end |
| Saga compensation: PaymentFailed -> CANCELLED | SHIPPED, 2s scenario green |
| Saga compensation: InventoryRejected -> Refund -> CANCELLED | SHIPPED, 6s scenario green |
| Saga compensation: ShipmentFailed -> Release -> Refund -> CANCELLED | SHIPPED, ~6s scenario green |
| Debezium connector wired + projection-service consumes CDC events | SHIPPED, end-to-end scenario green in 3s |
| Saga + outbox metrics (Micrometer) | SHIPPED |
| Grafana dashboards (saga-health + outbox-health) | SHIPPED, auto-provisioned |
| OpenTelemetry tracing | DEFERRED (returns with chaos suite where multi-hop debugging earns the setup cost) |
| Chaos test suite (5 scenarios via Toxiproxy) | PLANNED (week 8) |
| k6 load test + audit | PLANNED (week 9) |
| K8s manifests | PLANNED (week 10) |
| Outbox poller (sharded, `SKIP LOCKED`) | PLANNED (week 3) |
| Saga happy-path orchestration | PLANNED (week 4) |
| Saga compensation flows | PLANNED (week 5) |
| Debezium connector registered + projection-service | PLANNED (week 6) |
| Observability instrumentation + custom metrics | PLANNED (week 7) |
| Chaos test suite (5 scenarios) | PLANNED (week 8) |
| k6 load test + audit | PLANNED (week 9) |
| K8s manifests (kustomize + Strimzi) | PLANNED (week 10) |
| All 7 ADRs written | PLANNED (1 stub now, others week 2-10) |

Treat anything not SHIPPED as not-yet-real.
