# Generation durable store

`store` owns the PostgreSQL state for Generation. It records durable intent
before external work and keeps each state transition, quota reservation, and
Platform outbox event in the same transaction where the contract requires
atomicity. It does not call Storage, Docker, or Temporal; adapters perform those
calls from the persisted intents and record their exact receipts here.

## Lifecycle

1. `Admit` validates Contracts `generation/v1`, persists the exact normalized
   request and parameter bytes, reserves queue capacity, and creates the
   seven-day idempotency receipt. A matching live key replays the existing job;
   a changed request conflicts.
2. `BeginPinAcquireIntent` persists the exact Storage pin request before the
   call. `RecordPinAcquireResponse` records its response. Once active,
   `MarkPinActiveAndRecordWorkflowStart` commits the pin and stable
   `render/{jobID}` workflow intent together. Dispatchers claim persisted
   workflow intents and retry that stable ID until `MarkWorkflowStarted`
   acknowledges it. While preparing, the worker may persist and activate a
   second pin for the recorded derived snapshot with `MarkPinActive`.
3. `ClaimExecution` atomically moves queue capacity to an execution slot and
   creates a new attempt ID and fencing epoch. The caller persists exact
   operation requests with `SaveIntent` before calling external systems. It
   records container identity before start and calls `MarkAttemptStopped` only
   after the exact container is confirmed stopped. An unknown Docker state
   continues to hold the execution slot. A janitor can record a recovered
   container only against the original attempt epoch and create-intent key.
4. A completed render moves to a separate publication lease. A publisher saves
   upload/finalization intents, then records the immutable candidate with
   `RegisterCandidate`. A later publisher may adopt that candidate after lease
   expiry. `TerminalSuccess` verifies the active publication epoch, candidate,
   receipt, and cancellation state, then commits the artifact, terminal status,
   cleanup intent, and completion event atomically.
5. `RequestCancel`, `FailJob`, `ExpireJob`, and deadline reconciliation persist
   terminal events in the same transaction as their state changes. Cleanup
   callers claim durable cleanup intents and record idempotent receipts.

Database time is authoritative for deadlines, leases, and retention. Quota
changes use a transaction-scoped advisory lock so concurrent admissions cannot
exceed global or workspace limits. The default bounds are 50 queued jobs
globally / 10 per workspace and 2 executing jobs globally / 1 per workspace.

## Retention and owner checks

Admission receipts expire after seven days. After 24 hours in a terminal state,
`Retain` clears parameter bytes, the normalized request, raw diagnostics, and
sensitive derive/render intent request and output copies. Unfinished sensitive
intents are scrubbed and receive a separate durable abandonment cleanup intent.
It removes job history after 30 days only when no receipt, reservation,
unresolved operation or cleanup intent, pending workflow start, active pin,
unsent outbox event, or quarantined inbox event remains. Storage owner callbacks
use exact scope and identity matches; database errors are returned as errors so
Storage can treat them as unknown ownership.

## Tests

The integration tests use a real PostgreSQL database selected by
`GENERATION_TEST_DATABASE_URL`. In the E04 work environment, dot-source
`..\infra\scripts\e04-test-env.ps1` from the Generation module directory before
running:

```powershell
go test ./internal/store -count=1
go vet ./internal/store
```

The tests exercise concurrent quota admission, durable replay and source/derived
pin ownership, workflow claim recovery, stale fencing, unknown-container slot
retention and janitor recovery, candidate adoption, publisher takeover, atomic
terminal/outbox writes, cancellation races, and retention scrubbing and blockers.
