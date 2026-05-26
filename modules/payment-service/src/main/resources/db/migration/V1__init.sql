-- payment-service V1: payments + outbox + processed_events.
-- See docs/adr/0002. Debezium tails business tables via per-database connectors; outbox
-- is excluded from every connector's table.include.list to avoid double-publishing.

CREATE TABLE payments (
    id              BIGSERIAL PRIMARY KEY,
    payment_uuid    UUID         NOT NULL UNIQUE,
    order_uuid      UUID         NOT NULL,
    amount_cents    BIGINT       NOT NULL CHECK (amount_cents >= 0),
    currency        VARCHAR(3)   NOT NULL,
    status          VARCHAR(32)  NOT NULL,
    gateway_ref     VARCHAR(128),
    idempotency_key UUID         NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX payments_order_idx  ON payments (order_uuid);
CREATE INDEX payments_status_idx ON payments (status);

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

CREATE INDEX outbox_unpublished_idx
    ON outbox (shard_key, id)
    WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id        UUID         PRIMARY KEY,
    consumer_group  VARCHAR(64)  NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX processed_events_group_idx ON processed_events (consumer_group, processed_at);
