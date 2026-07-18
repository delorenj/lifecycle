# Lifecycle Authority

Lifecycle is the standalone deterministic authority for 33GOD lifecycle
specifications, operational state, legal transitions, frontier, obligations,
and actor capabilities. Bloodbank owns the canonical wire schemas and
NATS/JetStream transport. Candystore may project the emitted event history but
is never this service's operational database.

The implementation preserves the evaluator extracted from Bloodbank; see
[`docs/extraction-provenance.md`](docs/extraction-provenance.md). The pinned
contract surface is recorded in
[`contracts/bloodbank-v1.lock.json`](contracts/bloodbank-v1.lock.json).
The original shared-database SQL remains recoverable in extraction history;
only the forward, checksummed files under `migrations/` are present as
executable runtime truth. Migration `0001` requires a clean Lifecycle-owned
database and fails closed if it finds the extracted shared controller schema;
legacy cutover is an explicit audited export/import, never an in-place guess.

## Runtime contract

The image has one entrypoint with four explicit modes:

```text
python -m main migrate
python -m main bootstrap ... --as-of <RFC3339>
python -m main serve
python -m main healthcheck --url http://127.0.0.1:8080/readyz
```

`LIFECYCLE_DATABASE_URL` is required. There is intentionally no Candystore or
shared-database default. `BLOODBANK_NATS_URLS` defaults to local NATS only;
production should set it explicitly. Service mode refuses to start until all
forward migrations are current.

- `/livez` proves only that the process and HTTP loop are alive.
- `/readyz` proves current migrations, PostgreSQL access, NATS connectivity,
  both canonical Bloodbank streams, and both durable consumer bindings.
- `/metrics` exposes small process counters for command verdicts, consumer
  retries, reconnects, and outbox delivery.

## Authority invariants

- Reconciliation uses only caller/source `as_of` time and stable input ordering.
- Autonomous and supervised modes apply computed status; manual mode holds the
  current status while still updating health; disabled mode holds status as
  intentional nominal non-progress. Missing production observations are
  degraded/stale, never rendered as an empty healthy state.
- Every mutating command carries actor/capability context, an idempotency key,
  and exact `expected_state_version`.
- The lifecycle state row is locked before version/capability/frontier checks.
- State, append-only history, command result, and canonical outbox envelopes
  commit in one PostgreSQL transaction.
- Stale, unauthorized, malformed, and illegal commands never mutate state or
  history. An applied retry returns `idempotent` with the original event/version.
- Source `bloodbank.v1.repo.task.recorded` identity, time, provenance, ordering
  key, payload, and payload hash are preserved without interpreting provider
  columns as lifecycle truth.
- NATS acknowledgement happens after PostgreSQL commit. Publisher failure never
  rolls back committed authority state, append-only history, idempotency records,
  or outbox envelopes.
- Bloodbank delivery is at-least-once. Stable CloudEvent IDs, command identity,
  observation identity, and the append-only idempotency ledger make redelivery
  safe without claiming exactly-once transport.
- There is one post-bootstrap state mutation path (`LifecycleAuthority`) and one publisher
  (`JetStreamRuntime`); no direct SQL dogfood writer or compatibility publisher
  is shipped in the production tip.
- Outbox workers publish only the earliest unpublished sequence for each
  lifecycle. Later effects wait behind a failed/backed-off predecessor while
  unrelated lifecycle aggregates continue independently.

## Development validation

```bash
uv sync --all-extras --dev
mise run check
LIFECYCLE_RUN_INTEGRATION=1 uv run pytest -m integration
```

Integration tests create uniquely named PostgreSQL/NATS containers, network,
ports, and volumes. Their teardown removes only those exact test-owned resources.

Build a local image with source labels:

```bash
mise run image:build
```

Integrated deployment consumes an immutable GHCR digest. This component does
not define root Compose; the 33GOD platform layer owns that composition.

## Current 33GOD integration

The implemented 33GOD local slice runs revision
`715ab2ea62bcece488c8d6029869af8d3651c39a` from the immutable image
`ghcr.io/delorenj/lifecycle@sha256:e391a8aab13ca582e2026846a268a6a228c7b63c25e5d469255572e4b2988526`.
Root Compose supplies a dedicated PostgreSQL authority volume and runs the
published CLI in this fail-closed order: `migrate`, deterministic `bootstrap`,
then `serve`. It does not rebuild or substitute the image.

Lifecycle consumes observations and commands and publishes snapshots and stable
command verdicts through Bloodbank's canonical JetStream streams. Candystore's
durable consumers replay those publications into a read-only projection. Momo
may rank the returned legal frontier and resolve authoritative obligation skill
references; Holocene may render that projection and submit high-level commands.
Neither client, Candystore, Bloodbank, nor root Compose derives or writes
Lifecycle truth.

The exercised local integration proves restart catch-up without duplicate
transition effects, rejection without mutation for stale versions and invalid
capabilities, NATS outage recovery with ordered eventual outbox publication,
and dedicated PostgreSQL persistence across service and database-process
restarts. Hosted/cloud deployment, multi-tenant authorization, and release-tag
promotion remain future work and are not implied by this local slice.
