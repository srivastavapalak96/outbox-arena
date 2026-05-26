# ADR-0007: No Debezium Outbox Event Router SMT

Date: week 6
Status: Accepted

## Context

Debezium ships a Single Message Transformation (`io.debezium.transforms.outbox.EventRouter`)
purpose-built for the outbox pattern. With the EventRouter SMT, Debezium reads
the outbox table directly from the WAL, transforms each row into a Kafka
message (using configurable column names for the event type, aggregate ID,
payload, etc.), and publishes to a dynamically routed topic. The application
poller goes away entirely.

It is, by any reasonable measure, the *more elegant* solution:

- Fewer moving parts: no `OutboxPoller`, no `OutboxPollerScheduler`, no
  `outbox.publish.*` metrics to track.
- Lower publish latency: the WAL tailer picks up the row as soon as the
  Postgres commit lands, with no poll interval to tune.
- No `SELECT FOR UPDATE SKIP LOCKED` because there are no application-level
  contenders for the rows.
- Battle-tested at scale by every team that runs Debezium in production.

We deliberately do NOT use it.

## Decision

The outbox poller is an explicit application-layer service that publishes
outbox rows to Kafka. The Debezium connector reads the WAL for *business
tables* (`orders`, `order_items`, `payments`, `shipments`) and emits to
`cdc.public.*` topics. The two flows are physically separate.

## Why

The decision is pedagogical, not technical.

The single most important architectural claim in this repo is the one from
ADR-0002: **the Outbox pattern and CDC are two different things**. Outbox
publishes *commands* (an application's intention to trigger another action).
CDC publishes *derived state* (the consequence of a row changing in a source
table).

Folding the outbox into Debezium collapses the two patterns into one in the
diagram and in the code. Reviewers see one box (Debezium) where there should
be two (poller + Debezium). The conceptual asymmetry that motivates
*everything else* in this repo -- the consumer-side idempotency layer, the
sharded SKIP LOCKED, the synchronous send semantics, the explicit reasoning
about producer-session continuity -- becomes invisible.

In a production system at $JOB, the EventRouter SMT is often the right
choice. It removes operational surface. But the value of this repo is
demonstrating the patterns, not minimising operational surface. The explicit
poller is the surface that lets reviewers see:

1. **Producer-side idempotency vs Kafka idempotence.** The poller's
   synchronous `kafkaTemplate.send().get()` and its `acks=all` +
   `enable.idempotence=true` config are visible code. The reasoning about
   why these aren't sufficient (poller restart = new producer ID = no
   cross-session dedup) is right there in the javadoc.
2. **Sharding for horizontal scale.** The `shard_key SMALLINT` column and
   the partial index `(shard_key, id) WHERE published_at IS NULL` together
   demonstrate a real scaling pattern. The EventRouter SMT does its own
   parallelism inside Connect, but the user doesn't see it; the pattern
   isn't visible in the application code.
3. **Failure-mode boundary.** When a poller fails, where does the failure
   show up? The `outbox.publish.failures` counter. The `outbox.unpublished.count`
   gauge. The dead-letter table (planned). All of this is invisible
   infrastructure-as-a-service if Debezium does the publishing.

There is also one technical reason to keep the poller: it gives us a place
to put logic that's awkward inside a Connect SMT. Examples:

- Routing decisions that depend on application state (e.g., "if the order
  total is over $50,000, also publish to a fraud-review topic"). Trivial in
  Java, painful in an SMT.
- Per-tenant sharding policies that change at runtime.
- Synthetic correlation IDs that require reading other tables (e.g., joining
  the outbox row with the order's correlation ID).

We don't need any of those today, but the architectural decision is to keep
the option open.

## Consequences

- The Debezium connector's `table.include.list` deliberately excludes the
  `outbox` table on every saga-participant database. Including it would
  result in double-publishing -- once by our poller, once by Debezium.
  This exclusion is the most important line in
  `infra/kafka-connect/debezium-connector.json`.
- Operational surface is higher than it needs to be. We accept this as the
  cost of pedagogical clarity.
- Adding a new business table that should be tailed by CDC requires only a
  Flyway migration plus an update to `table.include.list`. Adding a new
  service-with-outbox requires the same five files as before plus nothing
  in the Debezium config.
- The README's architecture diagram has two parallel arrows from
  order-service to Kafka: one via the Outbox poller (commands.payment.v1)
  and one via Debezium (cdc.public.orders). Reviewers see the asymmetry
  immediately.

## What changes if this ADR is reversed

If a future maintainer decides the operational surface isn't worth the
clarity:

1. Add `transforms.outbox.type=io.debezium.transforms.outbox.EventRouter`
   to the connector config. Configure the column names.
2. Add `outbox` to `table.include.list` (or remove the exclusion).
3. Delete `OutboxPoller`, `OutboxPollerScheduler`, `OutboxAutoConfiguration`,
   `OutboxPollerProperties`, the metrics in `OutboxBacklogGauge`.
4. Update ADR-0002 and ADR-0005 to reflect that the command plane is now
   also Debezium-published, then explain how the application-vs-infra
   boundary is still meaningful.
5. Rewrite ADR-0006 because consumer-side idempotency is still required
   (Debezium ack + Postgres UPDATE ordering still has the same
   producer-restart-new-producer-id race).

The point of writing this all out is to make the cost of reversal explicit.
A future maintainer can make that call -- but they should understand what
they're trading away.

## References

- Debezium docs, Outbox Event Router:
  https://debezium.io/documentation/reference/transformations/outbox-event-router.html
- Chris Richardson, "Pattern: Transactional outbox" -- the canonical
  description that predates the SMT and matches our explicit approach
- ADR-0002, the parent decision this builds on
