# E04 asynchronous rendering

Build with Go 1.27.0 and the exact Core/Contracts/Platform/Integrations revisions
in `ci/dependencies.json`. During draft delivery, Infra supplies an external
workspace; published `v0.1.0` modules do not yet contain the E04 APIs. The existing
module path `github.com/Feruum/Leafrun` and local `Render` signature are retained.
Draft comparison against `main` remains temporary until `develop` exists.

## Processes

`cmd/leafrun-generation` accepts one role:

| Role | Responsibility |
| --- | --- |
| `migrate` | Own PostgreSQL schema and Platform event tables |
| `api` | Admission, job status/cancel, owner checks, health/readiness |
| `workflow` | Stable `render/{jobId}` Temporal orchestration |
| `dispatcher` | Durable workflow-start intent delivery |
| `reconcile` | Pending admission recovery, deadlines and retention |
| `controller` | Execution activity, fenced Docker lifecycle and publication |
| `janitor` | Stale attempts, unknown-container recovery and durable cleanup |
| `event-relay` | Own outbox to JetStream; mark sent after broker ACK |

Each role validates its own required configuration before starting. Database
roles receive `GENERATION_DATABASE_URL`; Temporal roles receive
`GENERATION_TEMPORAL_ADDRESS` and namespace (default `leafrun-generation`). API
and controller configuration requires Storage/Access URLs and credentials;
owner callbacks use a separate credential. The API's credential document maps
tokens to current subject/domain principals. No missing checker grants access.
The internal stand explicitly supplies a development Access fixture.

The controller and janitor additionally require an immutable renderer image ID,
private staging root and Docker connection. Renderer images contain only Typst
0.15.1, `leafrun-renderer` and the independent PID 1 supervisor. They receive no
network, credentials, Docker socket or host bind mounts. Container input/output
is a bounded framed stream attached before start with logging disabled.

## Durable lifecycle

Admission fingerprints the complete normalized request in workspace/initiator
scope, including project. Current credentials and access are checked before
every replay. A 202 receipt follows a durable queue reservation, activated source
snapshot pin and durable workflow-start intent. Repeated matching requests retain
the same job ID; changed input under a live key conflicts. Lost external ACKs
recover through saved requests and receipts.

The renderer restores the captured bundle, validates parameters against a closed
immutable schema resolver, derives independent `input.json`, then renders. The
controller accepts output only after full framing/terminal verification and
confirmed exit 0. Upload/finalize requests are persisted before each external
effect. Publication has a separate fenced epoch; takeover adopts prior uploads
and finalized receipts without rerendering. Current access, cancellation, exact
bindings and the original fixed 24-hour artifact expiry gate the atomic success,
artifact binding and outbox transaction. A finalize receipt alone grants no
download access.

Compile, schema validation, cancellation and resource limits are terminal. Only
eligible technical failures consume another actual render attempt. Temporal
control/lease retries are independent of that three-attempt budget and bounded
by the job deadline. Unknown Docker state retains slots and staging. Janitor
confirms the exact container identity and stopped state before cleanup/retry.
Source and derived pins release through durable cleanup intents.

## Limits and retention

Attempts have one CPU, 512 MiB memory, 256 MiB tmpfs and a 60-second independent
watchdog. SQL leases last 30 seconds with heartbeats at most 10 seconds apart.
The first durable admission starts a 15-minute deadline. Queue limits are 50
globally / 10 per workspace; executing slots are 2 / 1. Overflow returns 429.

HTTP input is 1 MiB; source and derived snapshots are each 100 MiB; PDFs 25 MiB;
schema resources 4 MiB; bundle metadata 12 MiB; wire input/output 128/160 MiB.
JSON normalization preserves numeric lexemes, rejects duplicate keys, invalid
Unicode, excessive nesting, integer overflow and nonfinite floating values.
Schema loading permits captured files only and never fetches HTTP or host files.

Parameters, diagnostics and sensitive intent copies expire 24 hours after
terminal state. Idempotency lasts seven days. Jobs and resolved event history
expire after 30 days only without unresolved dependencies. Pending outbox,
cleanup and quarantine survive age-based retention. Source/derived snapshot
bytes use Storage's reference/pin/GC lifetime; PDF pins never extend fixed TTL.

## Verification

In the complete development workspace:

```powershell
. ../infra/scripts/e04-test-env.ps1
go test ./... -count=1
go vet ./...
go build ./...
```

Use Infra's full CI script for real PG/S3/Temporal/NATS round trips, restart
persistence, controller death and watchdog recovery, plus Linux race/vet/build
across all six repositories. Unit and database regressions cover concurrent
idempotency/quotas, lost ACKs, stale fencing, cancellation races, publication
takeover without local staging, strict snapshot receipts and retention guards.
E04 maps LEA-26–32; parameters/schema implement only part of LEA-36/37. Full E05
profiles/releases/DAG/Batch/callback/cache remain outside this delivery.
