-- order-service V1: orders, order_items, sagas, outbox, processed_events.
-- See docs/adr/0002 for the Outbox vs CDC plane boundary that motivates this schema.
-- Debezium's table.include.list will tail orders and order_items here; outbox is excluded.

CREATE TABLE orders (
    id                 BIGSERIAL PRIMARY KEY,
    order_uuid         UUID         NOT NULL UNIQUE,
    buyer_id           VARCHAR(64)  NOT NULL,
    status             VARCHAR(32)  NOT NULL,
    total_amount_cents BIGINT       NOT NULL,
    currency           CHAR(3)      NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    version            BIGINT       NOT NULL DEFAULT 0
);

CREATE INDEX orders_buyer_idx     ON orders (buyer_id);
CREATE INDEX orders_status_idx    ON orders (status);
CREATE INDEX orders_created_idx   ON orders (created_at DESC);

CREATE TABLE order_items (
    id                 BIGSERIAL PRIMARY KEY,
    order_id           BIGINT       NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    seller_id          VARCHAR(64)  NOT NULL,
    sku                VARCHAR(64)  NOT NULL,
    qty                INTEGER      NOT NULL CHECK (qty > 0),
    unit_price_cents   BIGINT       NOT NULL CHECK (unit_price_cents >= 0)
);

CREATE INDEX order_items_order_idx  ON order_items (order_id);
CREATE INDEX order_items_seller_idx ON order_items (seller_id);

-- Saga state lives in order-service since order-service is the orchestrator (ADR-0004).
CREATE TABLE sagas (
    saga_id        UUID         PRIMARY KEY,
    order_id       BIGINT       NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    state          VARCHAR(32)  NOT NULL,
    current_step   VARCHAR(64)  NOT NULL,
    retries        INTEGER      NOT NULL DEFAULT 0,
    last_error     TEXT,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX sagas_state_idx ON sagas (state);
CREATE INDEX sagas_order_idx ON sagas (order_id);

-- The Outbox. Same schema in every saga-participant service.
-- event_id is the producer-side idempotency key. shard_key allows safe horizontal
-- scaling of the poller (multiple pollers on different shards, no contention).
CREATE TABLE outbox (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID         NOT NULL UNIQUE,
    aggregate_type  VARCHAR(64)  NOT NULL,
    aggregate_id    VARCHAR(64)  NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    payload         JSONB        NOT NULL,
    headers         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    shard_key       SMALLINT     NOT NULL
);

-- Partial index: only rows the poller cares about. Stays small even as outbox grows.
CREATE INDEX outbox_unpublished_idx
    ON outbox (shard_key, id)
    WHERE published_at IS NULL;

-- Consumer-side idempotency. INSERT with PK conflict = "already processed, drop event."
CREATE TABLE processed_events (
    event_id        UUID         PRIMARY KEY,
    consumer_group  VARCHAR(64)  NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX processed_events_group_idx ON processed_events (consumer_group, processed_at);
