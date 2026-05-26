-- inventory-service V1: per-seller stock + reservations + outbox + processed_events.
-- inventory is keyed on (sku, seller_id) so two sellers can offer the same SKU independently.

CREATE TABLE inventory (
    sku        VARCHAR(64) NOT NULL,
    seller_id  VARCHAR(64) NOT NULL,
    on_hand    INTEGER     NOT NULL CHECK (on_hand >= 0),
    reserved   INTEGER     NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    version    BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (sku, seller_id)
);

CREATE TABLE inventory_reservations (
    id                BIGSERIAL PRIMARY KEY,
    reservation_uuid  UUID         NOT NULL UNIQUE,
    order_uuid        UUID         NOT NULL,
    sku               VARCHAR(64)  NOT NULL,
    seller_id         VARCHAR(64)  NOT NULL,
    qty               INTEGER      NOT NULL CHECK (qty > 0),
    status            VARCHAR(32)  NOT NULL,
    expires_at        TIMESTAMPTZ  NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX reservations_order_idx  ON inventory_reservations (order_uuid);
CREATE INDEX reservations_status_idx ON inventory_reservations (status);

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
