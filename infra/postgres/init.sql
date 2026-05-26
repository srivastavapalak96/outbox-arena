-- Bootstrap: per-service databases and owners.
-- Each saga participant gets its own logical database so DB-level isolation matches service-level
-- isolation. Debezium connects with the postgres superuser to read the WAL across all of them.
-- Run automatically by the Postgres container's docker-entrypoint-initdb.d at first boot.

CREATE ROLE order_svc      WITH LOGIN PASSWORD 'order_svc'      CREATEDB;
CREATE ROLE payment_svc    WITH LOGIN PASSWORD 'payment_svc'    CREATEDB;
CREATE ROLE inventory_svc  WITH LOGIN PASSWORD 'inventory_svc'  CREATEDB;
CREATE ROLE shipping_svc   WITH LOGIN PASSWORD 'shipping_svc'   CREATEDB;
CREATE ROLE projection_svc WITH LOGIN PASSWORD 'projection_svc' CREATEDB;

CREATE DATABASE order_svc      OWNER order_svc;
CREATE DATABASE payment_svc    OWNER payment_svc;
CREATE DATABASE inventory_svc  OWNER inventory_svc;
CREATE DATABASE shipping_svc   OWNER shipping_svc;
CREATE DATABASE projection_svc OWNER projection_svc;

-- Debezium-specific role with REPLICATION privilege.
-- Used by the Kafka Connect connector to read the WAL via pgoutput.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT CONNECT ON DATABASE order_svc, payment_svc, inventory_svc, shipping_svc TO debezium;

-- Grant schema-level read on each saga DB so Debezium can do the initial snapshot.
-- Each ALTER DEFAULT PRIVILEGES applies to *future* tables created by the service owners,
-- which means migrations run by Flyway. The connector is configured with table.include.list
-- restricted to business tables (see ADR-0002); outbox is never tailed.
\connect order_svc
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

\connect payment_svc
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

\connect inventory_svc
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

\connect shipping_svc
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
