-- shipping-service V1: shipments + outbox + processed_events.

CREATE TABLE shipments (
    id              BIGSERIAL PRIMARY KEY,
    shipment_uuid   UUID         NOT NULL UNIQUE,
    order_uuid      UUID         NOT NULL UNIQUE,
    carrier         VARCHAR(32)  NOT NULL,
    tracking_no     VARCHAR(64),
    status          VARCHAR(32)  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX shipments_status_idx ON shipments (status);

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
