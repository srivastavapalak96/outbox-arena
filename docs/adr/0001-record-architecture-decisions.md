# ADR-0001: Record architecture decisions

Date: week 1
Status: Accepted

## Context

We need a lightweight way to capture architecturally significant decisions in this repo --
ones that affect structure, dependencies, interfaces, or build configuration -- so the
reasoning behind a given choice is preserved even when the author has long forgotten it.

## Decision

Use a single-file-per-decision Architecture Decision Record (ADR) format derived from
Michael Nygard's original write-up. ADRs live under `docs/adr/` and are numbered
sequentially. Each ADR has the structure shown here: Context, Decision, Consequences.

ADRs are immutable once accepted; supersede an old ADR by writing a new one that
explicitly references and obsoletes it.

## Consequences

- New contributors can read `docs/adr/` and understand the path of decisions.
- Decisions become discoverable rather than tribal.
- A small overhead per significant decision -- one short Markdown file.

The next ADRs (0002 onward) will cover: the Outbox / CDC plane boundary, PostgreSQL over
MySQL, orchestration saga over choreography, the sharded outbox poller, Postgres
`processed_events` over Redis, and why we deliberately do NOT use the Debezium Outbox SMT.
