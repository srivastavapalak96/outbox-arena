# Status

Last updated: week 1.

| Area | Status |
|------|--------|
| Gradle multi-module skeleton | SHIPPED, `./gradlew build` green |
| 6 Spring Boot service modules (boot + actuator only) | SHIPPED, order-service verified locally on 8081 |
| Docker Compose: Postgres 16 with `wal_level=logical` | SHIPPED, healthy |
| Docker Compose: Kafka 3.7 KRaft, single broker | SHIPPED, healthy |
| Docker Compose: Kafka Connect with Debezium 2.7.3 | SHIPPED, healthy |
| Docker Compose: Prometheus + Grafana + Jaeger | SHIPPED, all reachable |
| Flyway migrations | PLANNED (week 2) |
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
