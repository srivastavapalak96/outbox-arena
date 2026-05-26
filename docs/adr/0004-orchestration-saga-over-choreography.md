# ADR-0004: Orchestration saga over choreography

Date: week 4
Status: Accepted

## Context

A multi-step saga (payment + inventory + shipping) can be coordinated in
two fundamentally different ways:

- **Choreography**: participants listen to each other's events directly.
  `payment-service` emits `PaymentAuthorized`, `inventory-service` listens
  and on success emits `InventoryReserved`, `shipping-service` listens and
  on success emits `ShipmentDispatched`. There is no central coordinator;
  the saga is implicit in the event graph.

- **Orchestration**: one designated service (the orchestrator) listens to
  every participant's reply, advances a saga state machine in its own
  database, and emits the next command. `order-service` here is the
  orchestrator -- it listens to `PaymentAuthorized` and emits
  `InventoryReservationRequested`; listens to `InventoryReserved` and
  emits `ShippingRequested`; and so on.

Both are valid. They make different tradeoffs.

## Decision

Orchestration via `order-service`. The saga state machine lives in
`orders.status` and `sagas.state` (the latter for compensation tracking
state that isn't strictly the order's status), and every state transition
is implemented as a `@KafkaListener` method in
`OrderSagaOrchestrator.java`.

## Why

### 1. Visibility

With orchestration, the entire saga state is materialised in one table.
A reviewer (or a 4 AM oncall) can run:

```sql
SELECT order_uuid, status, current_step, retries, last_error
  FROM sagas
  JOIN orders ON sagas.order_id = orders.id
 WHERE status NOT IN ('COMPLETED', 'CANCELLED');
```

and see every in-flight saga. With choreography, the equivalent query
requires reconstructing state from the event log, joining across multiple
service databases, and reasoning about partial deliveries. Doable, but
expensive.

This argument is the single biggest reason. For a portfolio repo whose
value depends on demonstrating *how* to operate a saga in production, the
queryable-state property is decisive.

### 2. Compensation logic concentration

The compensation paths in this repo:

- `PaymentFailed` -> `CANCELLED`
- `InventoryRejected` -> issue refund -> `CANCELLED`
- `ShipmentFailed` -> release inventory -> issue refund -> `CANCELLED`

With orchestration, all three live as case branches in one file
(`OrderSagaOrchestrator.handle*Failed`). The reader sees the full
failure-handling matrix at a glance. With choreography, each compensation
step is distributed: `inventory-service` would need to know about
`PaymentRefundRequested`, which would couple it to payment-service's
event vocabulary; or the order-service would still need to broker the
compensation, at which point you have orchestration anyway.

### 3. Easier chaos testing

Orchestration concentrates failure-injection points. Kill the orchestrator
mid-saga and you can observe the resume-from-state behaviour by inspecting
`sagas.state`. With choreography, you'd kill arbitrary participants and
have to trace the event-graph health to know whether the saga is stuck or
just slow.

This matters specifically for the chaos suite in week 8/9. The
`chaos-broker-kill.sh` and (planned) SIGKILL scenarios are simpler to
specify because there's one place that owns "where is this saga right now."

### 4. Idempotent orchestrator restarts

Because the saga state lives in a single table the orchestrator owns,
restarting the orchestrator after a crash is a no-op for in-flight sagas.
The next event delivery causes the orchestrator to read the current
status, look at the incoming event's type, and either advance or skip
(via the `processed_events` dedup). No state to recover from event
history; no replay logic.

## Costs

The decision is not free. The orchestrator is a coordination bottleneck:

- Every saga transition writes the `orders` row, which means the
  orchestrator's database is a hot table at high throughput.
- Adding a new saga step requires changing `order-service`. A
  choreographed system might add the step by subscribing the new service
  to existing events without touching others.
- The orchestrator's queue is the bottleneck for per-order saga throughput
  (though sharded by `order_uuid`, so multiple orders proceed in parallel).

At portfolio scale (1,000 orders/sec target) none of these matters. At
Stripe scale, the orchestrator would itself be horizontally sharded -- a
mechanism we don't need to demonstrate here but is straightforward to add
later (consistent hash on `order_uuid` -> orchestrator partition).

## Why not the alternatives

### Choreography

The points above. Plus: choreography distributes the "what does it mean
for this saga to be complete" question across N services, each of which
needs to know about every other service's terminal events. The schema-
versioning matrix grows quadratically with the number of saga
participants.

### Hybrid (e.g., AWS Step Functions / Camunda / Zeebe)

External workflow engines are excellent in their own right, but for a
portfolio repo they hide the saga implementation behind another product.
The architectural point we want to make -- that every state transition
preserves dual-write atomicity by writing both the row update and the
next outbox row in one transaction -- is invisible inside a workflow
engine.

For a real production system, especially one with long-running steps and
human-in-the-loop approval gates, the workflow engine route is often
correct. Not for this repo.

## Consequences

- `order-service` is the only service that subscribes to *every* reply
  topic. Other services subscribe only to their own commands.
- The saga state machine is implemented as a six-method switch in
  `OrderSagaOrchestrator.java`. Adding a new transition is one new
  case branch.
- The `sagas` table is currently a placeholder (only the `state` column
  is updated); week 5 will use it for compensation retry tracking.
- The orchestrator inherits the same dual-write guarantees as every
  other saga participant: every state transition is `@Transactional` and
  writes both the order row UPDATE and the next-step outbox row in one
  go.

## References

- Chris Richardson, "Microservices Patterns" -- the orchestration vs
  choreography chapter
- The implementation: `modules/order-service/src/main/java/io/outboxarena/order/saga/OrderSagaOrchestrator.java`
- Empirical demonstration that compensation works end-to-end:
  `chaos/scenarios/compensation-{payment,inventory,shipping}-fail.sh`
