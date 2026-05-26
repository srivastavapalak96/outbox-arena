# ADR-0006: Postgres `processed_events` over Redis SETNX for consumer idempotency

Date: week 4
Status: Accepted

## Context

ADR-0005 establishes that the outbox poller cannot guarantee exactly-once
publish; a crash between Kafka ack and Postgres `UPDATE published_at` produces
a duplicate the broker cannot dedupe. The consumer side has to catch that case.
Consumer-side idempotency needs a deduplication store that maps "have I
processed this event-id before?" -> yes/no.

Two obvious candidates:

1. A Postgres table `processed_events (event_id UUID PK, consumer_group)`
   where the consumer inserts before doing the business write, and a
   duplicate-key violation means "already processed, skip."
2. A Redis `SETNX event_id 1 EX <ttl>` where success means first-time,
   failure means duplicate.

Both are obvious. They are not equivalent.

## Decision

We use the **Postgres `processed_events` table**, in the **same transaction as
the business write**.

## Why

The decisive factor is *atomicity with the side effect we're gating*.

Consumer logic is:

```java
@Transactional
public void onPaymentRequested(Event evt) {
    idempotent.once(evt.eventId, "payment-service", () -> {
        Payment p = new Payment(evt.orderUuid, evt.amountCents, ...);
        p.authorize();
        payments.save(p);
        outbox.save(new OutboxRecord("PaymentAuthorized", ...));
    });
}
```

The dedup check, the business write, AND the reply outbox row all need to
either commit together or roll back together. If they don't, we re-introduce
the dual-write problem at the consumer side -- now between the dedup store
and the business DB.

With **Postgres `processed_events` in the same transaction**:

| What can fail | What happens |
|---------------|--------------|
| Dedup `INSERT` collides (already processed) | Transaction is no-op'd, business write never runs |
| Business write fails | `processed_events` row rolls back too; next delivery retries cleanly |
| Service crashes mid-handler | Whole transaction rolls back; redelivery handled by Kafka offset commits |

With **Redis SETNX** as the dedup store:

| What can fail | What happens |
|---------------|--------------|
| SETNX succeeds, business write fails | Redis says "processed" but DB has no record. Permanent loss. |
| SETNX fails (network), retry SETNX succeeds, business write succeeds | Two business writes for one event |
| Business write succeeds, Redis ACK lost on the way back | Next delivery hits "duplicate", skips business write. Correct -- but only by accident of TTL not having expired. |

In other words: putting the dedup check in a separate system reintroduces the
dual-write problem at the consumer side. The pattern we're using to *solve*
dual-write at the producer side cannot itself be implemented on top of a
dual-write.

This argument generalises to any external dedup store -- DynamoDB conditional
write, etcd compare-and-swap, ZooKeeper exclusive create. None of them share
a transaction with the business write, so all of them carry the same race.

## Why not the alternatives

### Redis SETNX with retry

In practice teams paper over the race by careful retry logic and accepting
some duplicate writes. That's a real engineering choice, and at very high
throughput where the Postgres write rate is a bottleneck, it can be the right
one. For a portfolio repo that demonstrates the *correct* pattern, it's not.

### Idempotent business writes (no dedup table needed)

If every business operation is idempotent in its own right -- e.g., "set
payment.status = AUTHORIZED" is idempotent because re-execution writes the
same value -- then arguably the dedup table is unnecessary. The reality:

1. Most real saga steps are *not* idempotent. "Charge the credit card" is
   not idempotent without an idempotency key. "Decrement inventory" is not
   idempotent without an explicit reservation_uuid lookup.
2. Even when the business write *is* idempotent, the *outbox row* the
   consumer produces is not -- two consumer invocations write two outbox
   rows, the orchestrator advances state twice. The dedup table catches it
   centrally.

### A separate Postgres database for `processed_events`

Two databases, two transactions, two-phase commit -- back to the dual-write
problem. Same argument as Redis.

## Consequences

- Every saga-participant service has a `processed_events` table with the
  same schema, exposed via the shared `ProcessedEventRepository` in
  `modules/common`.
- `IdempotentConsumer.once(eventId, consumerGroup, work)` is the only public
  surface. Callers never touch `processed_events` directly; the wrapper
  guarantees the `INSERT` precedes the work.
- The `processed_events` table grows unboundedly. A nightly cleanup job
  (PLANNED, week 6 hardening) deletes rows older than 30 days. Until then
  the partial index on `(consumer_group, processed_at)` keeps lookups fast
  enough at portfolio scale.
- Adding a new consumer group requires no new infrastructure -- it just
  starts inserting with its own `consumer_group` discriminator.

## References

- The implementation: `modules/common/src/main/java/io/outboxarena/common/outbox/IdempotentConsumer.java`
- The class that uses it: `OrderSagaOrchestrator` (six listener methods,
  all wrapped in `idempotent.once(...)`)
- Empirical proof: the same `event-id` header re-delivered after a poller
  restart causes the second handler invocation to no-op (covered by the
  multi-poller race scenario, PLANNED for week 9)
- ADR-0005 -- the producer-side reasoning that motivates this ADR
