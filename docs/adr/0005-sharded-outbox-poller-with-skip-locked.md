# ADR-0005: Sharded outbox poller with `SELECT FOR UPDATE SKIP LOCKED`

Date: week 7
Status: Accepted

## Context

The Outbox pattern reduces the dual-write problem to a single durable write at
intake -- but the *publishing* side of the pattern is where production tradeoffs
live. A naive single-threaded poller works at low throughput; it stops working
the moment you need to either (a) scale horizontally for throughput, or (b)
tolerate the operator killing a service instance without losing in-flight rows.

We need a poller architecture that:

1. Lets multiple service instances run in parallel against the same outbox
   table without producing duplicate Kafka messages.
2. Recovers cleanly when an instance crashes between picking up rows and
   marking them published.
3. Stays sub-millisecond per sweep at portfolio scale (hundreds of unpublished
   rows visible at any moment), with a clear scaling path to tens of thousands.
4. Doesn't require a coordinator service, distributed lock manager, or anything
   else that becomes its own dependency.

## Decision

We use **per-shard polling** with **`SELECT FOR UPDATE SKIP LOCKED`** as the
mutual-exclusion primitive.

### Schema

Every outbox row carries a `shard_key SMALLINT` column. At intake time, the
producer computes `shard_key = hash(aggregate_id) mod NUM_SHARDS` (we use 16
shards). The partial index

```sql
CREATE INDEX outbox_unpublished_idx
    ON outbox (shard_key, id)
    WHERE published_at IS NULL;
```

keeps the index hot only for the rows the poller actually cares about. Even when
the outbox table grows to millions of historical rows, the unpublished slice
stays cheap to scan.

### Query

The poller runs, per owned shard, on a fixed-delay schedule:

```sql
SELECT * FROM outbox
 WHERE shard_key = :shard AND published_at IS NULL
 ORDER BY id
 FOR UPDATE SKIP LOCKED
 LIMIT 500;
```

`FOR UPDATE` acquires row-level locks on every returned row. `SKIP LOCKED`
(Postgres 9.5+) is the magic primitive: it tells Postgres to skip rows another
transaction is currently holding rather than blocking on them. Two pollers
running the same query against the same shard each receive a disjoint subset of
rows -- no duplicate work, no inter-poller coordination.

### Publish

For each locked row, the poller:

1. Builds the Kafka `Message` with `event_id` as both the message key (for
   per-aggregate ordering on a single partition) and the `event-id` header
   (for consumer-side deduplication).
2. Calls `kafkaTemplate.send(message).get()` -- synchronous, waits for broker
   ack. Asynchronous send would let the poller advance the row before the
   broker has the message, which is exactly the dual-write race the outbox
   pattern is designed to eliminate.
3. Marks `published_at = now()` on the row.

All three steps happen inside one `@Transactional(REQUIRES_NEW)` boundary. If
the publish fails, the transaction rolls back and the row stays unpublished;
the next sweep retries.

### Scheduler / poller split

The `@Scheduled` tick lives on a *separate bean* (`OutboxPollerScheduler`)
that holds a reference to `OutboxPoller`. This is the standard fix for Spring's
self-invocation gotcha: if both `@Scheduled` and `@Transactional` lived on the
same class, the scheduled method's call into the transactional method would
bypass the proxy and silently lose the transactional semantics. Splitting into
two beans makes the call go through the proxy.

## Why not the alternatives

### Single-threaded poller behind a leader-election lease

Doable, but it puts a hard upper bound on throughput (one machine's worth of
publish bandwidth). Recovery from leader failure adds operational complexity --
ZooKeeper/etcd leases, lease-renewal timers, fencing tokens. Sharding solves
the throughput problem and the recovery problem at once: shard ownership is
implicit (any instance can sweep any shard at any time; SKIP LOCKED prevents
collisions).

### Debezium Outbox Event Router SMT

Debezium ships an SMT that reads the outbox table directly and skips the
application poller entirely. We deliberately do NOT use it -- see ADR-0007. The
pedagogical point of this repo is to make the application poller visible.

### Application-level distributed lock (Redis SETNX)

Adds Redis to the dependency graph. Worse: introduces a *micro* dual-write
problem inside the very layer we're using to solve the *macro* one. If we
acquire a Redis lock, publish, then crash before releasing, the system either
hangs (lock TTL) or duplicates (lock released too early). Postgres row locks
are released by transaction abort automatically; there's nothing to leak.

## What this does NOT solve

The single most important sentence in this ADR:

> A poller crash between the Kafka broker's ack and the Postgres `UPDATE
> published_at` will cause the row to be re-published on the next sweep, and
> Kafka's `enable.idempotence` does **not** dedupe across producer sessions.

Each restarted poller instance is a *new producer* from Kafka's perspective
(new producer ID, fresh sequence numbers). So the broker sees the second send
as a new message, not a duplicate of the first. Consumer-side dedup via
`processed_events` (see ADR-0006) is what catches this case.

If you remember one thing from this ADR, remember that: **`enable.idempotence`
is not sufficient for outbox publishing; you need consumer-side dedup too.**
This is the subtle bit most "outbox demo" repos online get wrong by leaning
entirely on producer idempotence.

## Consequences

- One Postgres index per outbox table (partial, on `(shard_key, id)` where
  `published_at IS NULL`).
- The `event_id` UUID is the canonical idempotency key end to end: producer
  side via the `UNIQUE` constraint on `outbox.event_id`; over the wire via the
  `event-id` Kafka header; consumer side via the `processed_events` PK.
- Adding a new service that participates in the saga = adding the same five
  files (`outbox` table, `processed_events` table, same Flyway V1, same JPA
  entities from `modules/common`, same outbox-arena.outbox enabled config).
- The `OUTBOX_SHARDS=16` constant is currently set per-service in the
  producer; making it a config knob is an open item if a service ever needs
  to scale beyond 16 poller threads. At portfolio scale, 16 is already
  several orders of magnitude over need.

## References

- Postgres 9.5+ `FOR UPDATE ... SKIP LOCKED` docs
- Chris Richardson, "Pattern: Transactional outbox" (microservices.io)
- The class itself: `modules/common/src/main/java/io/outboxarena/common/outbox/OutboxPoller.java`
- The scheduler-vs-poller split rationale: `OutboxPollerScheduler.java` javadoc
- Empirical proof the design works under fault injection:
  `chaos/scenarios/chaos-broker-kill.sh` (100 events posted through a mid-flight
  broker outage, 0 lost)
