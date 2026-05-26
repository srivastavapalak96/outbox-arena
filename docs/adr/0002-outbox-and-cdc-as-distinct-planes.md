# ADR-0002: Outbox and CDC as distinct planes

Date: week 1
Status: Accepted

## Context

This project demonstrates two different solutions to the dual-write problem:
Transactional Outbox and Debezium CDC. A naive reading treats them as
interchangeable. They are not. Conflating them in the implementation makes the
architecture diagram smaller but the code less honest.

The dual-write problem itself: a service that needs to (a) persist a change to
its database and (b) publish an event about that change to a message broker
cannot do both atomically, because the two systems do not share a transaction.
Naive code "writes then publishes" and silently drops messages when the broker
is unreachable between the two steps.

## Decision

This repo treats the two patterns as separate planes, owned by different layers:

**Command plane (Outbox).**
- Owned by application code in each saga-participant service.
- Same database transaction writes the business row AND a row to the local
  `outbox` table.
- A separate poller (sharded, `SELECT FOR UPDATE SKIP LOCKED`) ships outbox rows
  to Kafka on topics named `commands.<aggregate>.v1`.
- Idempotency: producer side via `event_id` carried on every row; consumer side
  via `INSERT INTO processed_events (event_id, consumer_group)` in the same
  transaction as the business write. Duplicate-key violation = drop.
- Payload shape: explicit command DTOs (`OrderCreated`, `PaymentRequested`,
  `InventoryReserved`, ...). Schemas are owned by `modules/common`.

**Data plane (CDC).**
- Owned by infrastructure (Debezium connector running in Kafka Connect).
- Tails the Postgres WAL via `pgoutput` for tables `orders`, `order_items`,
  `payments`, `shipments` ONLY.
- The `outbox` table is *explicitly excluded* from `table.include.list`.
- Topics named `cdc.public.<table>` carry row-change events (before / after).
- Consumed by `projection-service` and only `projection-service`. Saga
  participants never consume CDC topics.

**The boundary, restated.** Outbox publishes commands -- "please do X." CDC
publishes derived state -- "row R changed from version n to n+1." Mixing them
would mean either:

- Saga participants would react to CDC events, coupling business logic to
  storage layout (a column rename becomes a saga break), or
- Read projections would be built from command events, requiring projectors
  to re-derive state machines that already exist in the source service.

Neither is acceptable.

## Why this matters in practice

- A `projection-service` that watches `commands.order.v1` would have to replay
  the entire saga state machine to know an order's current status. Watching
  `cdc.public.orders` instead, it just reads the row.
- A `payment-service` that listens to `cdc.public.orders` would couple itself
  to the order schema, the order-service's deployment, and the Debezium
  connector's filter config. Listening to `commands.order.v1` -> it just
  receives `PaymentRequested {orderUuid, amountCents}` and acts.

## Why NOT use Debezium's Outbox SMT (`io.debezium.transforms.outbox.EventRouter`)

It is tempting. The Outbox Event Router lets Debezium read the outbox table
directly, transform rows into Kafka events, and skip the application-layer
poller entirely. One less moving part.

We reject it for two reasons:

1. It collapses the two planes into one in the diagram. The whole pedagogical
   point of this repo is to show the boundary; removing the poller makes the
   boundary invisible.
2. The application poller is where production tradeoffs live (sharding,
   `SKIP LOCKED`, Kafka transactional producer, batch tuning, backpressure
   responses). Hiding it behind an SMT removes the surface that
   senior-engineer reviewers actually want to read.

In a real system shipping at $JOB, the SMT is a reasonable choice. In a
portfolio repo whose value is demonstrating the patterns, the explicit poller
is the correct choice.

## Consequences

- Two distinct types of Kafka topics: `commands.*` (application-emitted) and
  `cdc.*` (Debezium-emitted). The README diagram colours them differently.
- `projection-service` has no `outbox` table; it is read-only on its own DB.
- Saga participants must never consume `cdc.*` topics. Linted at the consumer
  level via topic-prefix configuration; enforced at the human level by ADR
  reference.
- The Debezium connector configuration must whitelist business tables and
  *exclude* `outbox`. Misconfiguring this would double-publish events and is
  the most likely operational error; the `table.include.list` enumerates the
  intent explicitly rather than using a negative filter.

## References

- Chris Richardson, "Pattern: Transactional outbox" (microservices.io)
- Debezium docs: "Outbox Event Router" -- the pattern we deliberately avoid
- This repo's `infra/kafka-connect/debezium-connector.json`
