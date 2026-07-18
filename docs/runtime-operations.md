# Runtime operations

## Migration and startup

Run `migrate` as a one-shot job before starting or updating service instances.
Migrations are forward-only, checksum recorded, and serialized with a
PostgreSQL advisory lock. A checksum mismatch fails closed. Each migration runs
in one database transaction, so a failed statement leaves neither a partial
schema change nor a migration-ledger row. The first standalone migration refuses
pre-existing extracted controller tables; move legacy facts through an explicit
audited export/import into a clean database.

Service mode never applies migrations implicitly. It starts only against a
current schema, then readiness continuously verifies schema state and database
access.

## NATS interruption

Authority mutations do not depend on a successful publish. During an outage,
the database transaction commits state/history/idempotency/outbox and the
publisher records bounded exponential retry metadata. On reconnect or process
restart, a publisher claims due rows with `FOR UPDATE SKIP LOCKED`, publishes
the unchanged canonical envelope with `Nats-Msg-Id=<CloudEvent id>`, waits for a
JetStream acknowledgement, and only then marks the row published.
Delivery is at-least-once. The publisher preserves per-lifecycle event sequence,
and JetStream deduplication uses the stable CloudEvent ID; neither is described
as exactly-once transport.

Operators should watch `/readyz`, `outbox_pending`, `outbox_retry`, and
`nats_reconnected`. A growing outbox with a healthy database is a transport
incident, not permission to edit lifecycle state or publish ad hoc envelopes.

## Poison and rejected inputs

Addressable malformed commands receive the canonical `malformed` reply and an
append-only command result. Bytes that lack the UUID/lifecycle/repo/version
identity required by the reply schema are terminated as poison and counted.
Transient database/transport failures are negatively acknowledged for durable
redelivery. Canonical but unbound repository observations are acknowledged and
ignored because the consumer sees the platform-wide repo task subject.
Obligation-completion evidence v2 is separately schema validated and durably
consumed. It is rejected unless it carries exact Momo source/producer identity,
the active `obligation_instance_id`, obligation and skill identity, target actor,
completion time, and a completed artifact. The occurrence identity and
`activated_at` are authority state persisted in the obligation projection.
Evidence observed before activation or for a prior occurrence remains auditable
input but cannot satisfy the current occurrence. An invocation request or
review-request event is not completion evidence.

Migration `0004_obligation_occurrence_projection.sql` upgrades an already-
persisted current occurrence from its state-history decision time. It fails
closed if that activation time or existing occurrence metadata is malformed;
it never substitutes a later reconcile-sweep timestamp.

## Concurrency and replay

All effects for one lifecycle serialize on its state row, while globally unique
command/event identities also serialize across aggregates. A queue generation is
deleted only if its worker, lease, and claimed `as_of` still match; an observation
that arrives during reconcile remains queued. Replay uses stored source times and
the same specification version. Unchanged replay produces no new state version,
history row, or transition publication.
