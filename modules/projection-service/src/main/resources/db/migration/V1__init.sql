-- projection-service V1: read-side denormalised view materialised from CDC events.
-- This service has NO outbox table and NO processed_events table -- it does not produce
-- events, only consumes the CDC stream. See docs/adr/0002.

CREATE TABLE order_views (
    order_uuid           UUID         PRIMARY KEY,
    buyer_id             VARCHAR(64)  NOT NULL,
    status               VARCHAR(32)  NOT NULL,
    total_amount_cents   BIGINT       NOT NULL,
    currency             VARCHAR(3)   NOT NULL,
    payment_status       VARCHAR(32),
    shipment_status      VARCHAR(32),
    tracking_no          VARCHAR(64),
    last_updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    source_lsn           VARCHAR(64)
);

CREATE INDEX order_views_buyer_idx          ON order_views (buyer_id);
CREATE INDEX order_views_status_idx         ON order_views (status);
CREATE INDEX order_views_payment_status_idx ON order_views (payment_status);
