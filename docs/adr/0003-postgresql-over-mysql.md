# ADR-0003: PostgreSQL 16 over MySQL

Date: week 1
Status: Accepted

## Context

The dual-write problem can be solved with the Outbox pattern on either
PostgreSQL or MySQL; both are widely deployed at scale. The author of this
repo has 4 years of production MySQL experience at Bosch (Tapzo, OneDirect),
so MySQL would be the path of least resistance. We deliberately chose
PostgreSQL anyway.

## Decision

PostgreSQL 16. All saga-participant services and the read-side projection
store run on the same Postgres instance (separate logical databases) in
local dev, with one schema per service in production.

## Why

Four reasons, in order of weight:

### 1. `SELECT FOR UPDATE SKIP LOCKED`

The single most important primitive in the entire repo. Postgres has had
clean, well-documented `SKIP LOCKED` semantics since 9.5 (released 2016).

MySQL added `SKIP LOCKED` in 8.0.1 (April 2018), and the implementation has
known sharp edges around gap locks under `REPEATABLE READ`. Real-world bug
reports from teams running sharded outbox pollers on MySQL 8 include
deadlock-with-self situations when row locking interacts with next-key
locks on a table with a covering index but stale statistics.

On Postgres 16 the same `FOR UPDATE SKIP LOCKED` query just works. No
isolation-level dance, no auto-explain to debug a phantom deadlock. For a
repo whose core architectural claim leans entirely on this primitive, that
matters.

### 2. `pgoutput` is the Debezium reference

The Debezium Postgres connector via the built-in `pgoutput` plugin is the
oldest and most-tested CDC path in the Debezium project. The MySQL connector
works but has more configuration surface (binlog format, GTID, server-id
collisions in test environments) and more subtle failure modes around DDL.

For a repo whose value depends on Debezium working reliably in a chaos
scenario, the reference path is the right one.

### 3. `JSONB` with GIN indexes

The outbox row's `payload` column is `JSONB`. Two operational queries we
want to be cheap:

```sql
-- "show me all unpublished outbox rows for order X" (debugging)
SELECT * FROM outbox WHERE payload @> '{"orderUuid": "..."}' AND published_at IS NULL;

-- "what's the publish lag distribution by event-type"
SELECT payload->>'eventType', percentile_cont(0.99) WITHIN GROUP (ORDER BY ...) FROM ...
```

Postgres JSONB is built for both. MySQL's JSON column type works but with
weaker indexing options (generated columns + indexed expressions instead of
GIN, more verbose syntax).

### 4. Logical replication slot lag as a first-class metric

Debezium's CDC consumer is the slowest-moving consumer in this stack; when
it falls behind, the downstream projection lags. Postgres exposes the
replication slot lag in bytes (`pg_replication_slots.confirmed_flush_lsn`),
which Prometheus can scrape directly. MySQL has equivalent info via
`SHOW REPLICA STATUS`, but it's farther from the standard
`pg_stat_*` view ecosystem that the postgres_exporter already covers.

### Honourable mention: pure laziness

Bosch MySQL 5.7 has subtly different `utf8mb4` collation semantics from
Postgres `UTF8`, the Hibernate dialect quirks differ, and so on. Choosing
Postgres meant fewer "I know how MySQL would do this" reflexes to retrain
during week 1 setup. This is a weak reason; the first three are real.

## Why not the alternatives

### MySQL 8

All the arguments above. Plus: MySQL doesn't have transactional DDL.
Schema migrations during a deployment can leave the database in a
half-migrated state if the deploy is interrupted; Postgres's transactional
DDL means a Flyway migration either succeeds entirely or rolls back
entirely. This isn't directly load-bearing for the outbox pattern, but it
makes the development loop faster.

### CockroachDB

CockroachDB supports `SKIP LOCKED` and is Postgres-wire-compatible. But the
Debezium CockroachDB connector is less mature than the Postgres one, and
running CockroachDB locally is heavier than Postgres. The portfolio-vs-
production tradeoff lands on the side of Postgres for now.

### MongoDB or DynamoDB

Both can implement the outbox pattern via a transaction across the
business document and an "events" subdocument. Both lose the `SKIP LOCKED`
primitive and would require a leader-elected poller instead. They also
lose the ability to use the same database for the consumer-side
`processed_events` dedup (ADR-0006), which would push that into a separate
system and reintroduce the dual-write problem. Not acceptable.

## Consequences

- Local infra runs `postgres:16-alpine` in Docker Compose.
- Connection pool: HikariCP, default sized at 20 connections per service.
- `wal_level=logical` in `postgresql.conf` (required by Debezium's
  `pgoutput` plugin).
- The Flyway migrations are Postgres-flavoured (`BIGSERIAL`, `JSONB`,
  partial indexes with `WHERE` clauses); porting to MySQL would touch
  every `V1__init.sql` file.
- The author has explicit experience with MySQL but not Postgres at scale.
  Choosing Postgres is a *deliberate skill investment* in the technology
  the rest of the repo's architectural decisions need.

## References

- Postgres 9.5 release notes (SKIP LOCKED introduction)
- Debezium Postgres connector documentation
- The query that depends on this: `OutboxPoller.sweepShard()`
  (`modules/common/src/main/java/io/outboxarena/common/outbox/OutboxPoller.java`)
