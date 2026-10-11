# Leafrun SaaS: design v1

> Historical design, retained from upstream. Superseded by the [current Leafrun architecture](https://github.com/leafrun/leafrun/blob/main/docs/architecture.md) and [roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md). As of October 9, 2026, executable code was simplified to `Render(repoPath)`. The HTTP engine and surrounding services discussed as implemented below belong to historical commit `d77d9a9`.

October 7, 2026. Draft for discussion and later implementation. [Miro board](https://miro.com/app/board/uXjVEd6scg8=/), [diagram index](board-index.md), [original v0](leafrun-saas-v0.md), [implementation stages](roadmap-v1.md).

SaaS features below were design decisions, not completed functionality. Plan numbers, retention and operational limits were proposed starting settings, not commercial commitments.

## 1. Product and readiness

Leafrun lets a person create a document from a template, edit sources, render a PDF and inspect history. A bot uses the same project through an API, supplying JSON for invoices, reports and other documents. Git stores source files, images, project fonts and change history.

| Capability | Historical server status | Intended development |
|---|---|---|
| Git → Typst → PDF | Implemented and checked | Reuse the engine in SaaS workers |
| Synchronous `POST /api/render` | Implemented | Keep an internal interface; add public `/v1` jobs |
| Cache, SHA, force-push, limits and cleanup | Implemented | Add queueing, revision pins and tenant isolation |
| Hosted Gitea and history import | Implemented | Manage projects through Leafrun |
| Public external Git projects | Implemented | Add workspace-specific credentials |
| Technical JSON logs | Implemented | Add user audit, search, metrics and request IDs |
| Markdown → PDF | Planned | Markdown adapter over Typst |
| Accounts, editor, teams and plans | Planned | Complete personal and company workflows |
| Automatic scaling | Planned | Worker and node scaling within a budget |

The [October 6 report](../superpowers/verification-2026-10-06.md) records engine evidence. No overall completion percentage was assigned: editor, access, billing and operations differ substantially in effort.

## 2. Architecture and alternatives

The historical starting choice was a modular Go application, PostgreSQL for data and queueing, hosted Gitea, private output storage and a separate render controller. The same application serves static TypeScript/editor assets. Modules need not be separate services in this earlier design.

| Option | Benefit | Cost | Historical decision |
|---|---|---|---|
| Go application, PostgreSQL queue, separate workers | Few mandatory services, simple transactions and debugging | Implement leases, cleanup and fairness | MVP choice |
| Broker and separate access, billing, Git and job services | Independent scaling and development ownership | More infrastructure and distributed failures | Split for measured reasons |
| Kubernetes and cloud services immediately | Placement and scaling mechanisms | Operational costs before users | Defer until justified by load |

| Module | Responsibility | Main dependency |
|---|---|---|
| Identity | Accounts, sessions, recovery, bot keys | PostgreSQL, email |
| Workspaces | Membership, roles, current permissions | Identity, PostgreSQL |
| Projects | Projects, templates, editor, history | Workspaces, Git |
| Git | Providers, commits, import, SHA resolution and pins | Gitea, external Git |
| Renders | Admission, state, cancellation, attempts | Git, Quotas, queue |
| Quotas | Entitlements, reservations, charges | PostgreSQL |
| Artifacts | Private PDFs, diagnostics, retention | Local or shared storage |
| Audit | Authorized action history | PostgreSQL |
| Billing | Plan assignment and payment events | Quotas, future payment adapter |

The controller claims a job, obtains its authorized source revision, starts an isolated Typst environment and publishes the result. The compiler gets no database, Git-provider, billing or other-job credentials.

## 3. Document formats and editor

Typst compiles `.typ`. Markdown needs a pinned adapter such as [cmarker](https://typst.app/universe/package/cmarker/) with `raw-typst: false`. Its documented CommonMark and embedded-Typst behavior must be checked against Typst 0.15.1 before implementation. Test extensions and formulas separately; do not promise arbitrary syntax support.

Project metadata stores `source_format` (`typst` or `markdown`), `entrypoint`, template and template version. Markdown gets a temporary `.typ` wrapper inside the working directory; its version enters the render manifest without changing the user's Git project. JSON uses the earlier engine's `sys.inputs.data_file` mechanism.

The first editor has a file tree, source pane, PDF pane, Save and Render actions, job state and bounded diagnostics. Rendering uses a saved commit. Unsaved changes must be saved before rendering; an old PDF cannot be presented as the result of unsaved text.

Rendering every keystroke was excluded from this MVP because it spends computation and free quota. Debounced preview, obsolete-job cancellation and separate preview compute policy were later work. Formatting, file import and PDF access belong to one project, with Gitea behind the UI.

## 4. Accounts, teams and access

A workspace owns projects and billing. A person can join multiple workspaces; each project belongs to exactly one. A company workspace has an organization name, owner, members and shared plan.

The MVP proposal uses email/password registration, email verification, login/logout and recovery. Server-side sessions issue `HttpOnly`, `Secure`, `SameSite` cookies; mutations enforce CSRF protection. Password-reset tokens are single-use, expire and revoke sessions. Hash passwords using a maintained Argon2id library and measured parameters, informed by [OWASP Password Storage](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html). SSO/OIDC is a later adapter in this historical design.

| Role | Permissions |
|---|---|
| Owner | Projects, members and roles, keys, plan, export and workspace deletion |
| Admin | Projects, connections, Editor/Viewer invitations, history and audit; no ownership transfer or billing |
| Editor | Authorized source reads, saving, rendering and results |
| Viewer | Authorized sources, history and PDFs; no render admission or file changes |

The first team plan grants workspace project access according to membership role. Per-project human grants were later work. MVP API keys can already restrict projects and actions such as `renders:create`, `renders:read` and `artifacts:read`.

Exactly one Owner must remain. Transfer to an existing member changes roles atomically and is audited. Invitations bind an email, single-use token and expiry; accepting rechecks and reserves member capacity so concurrent invitations cannot exceed the limit.

Owner/Admin creates a bot key. Show its secret once; store a hash and lookup prefix. Support expiry, revocation, ownership and secret-free usage history. Removing the owner or reducing their role reduces associated key rights. Every request checks current scope.

A URL `workspace_id` selects context but does not authorize access. Bind it to a verified session/key and current membership; carry the context into jobs, cache and downloads. Use composite workspace relationships and a restricted application database role, plus negative tests for PostgreSQL RLS, following the [OWASP tenant-isolation guidance](https://cheatsheetseries.owasp.org/cheatsheets/Multi_Tenant_Security_Cheat_Sheet.html). The queue controller has a separate system role with narrowly scoped claim operations.

Initial collaboration covers invitations, roles, shared projects, Git history and save conflicts. Simultaneous editing was deferred to a separate protocol after this historical MVP.

## 5. Git providers and saving

Two proposed project modes:

- `managed`: a private hosted-Gitea repository written by Leafrun's Git module; the editor enforces write quota.
- `external_read_only`: connected GitHub/GitLab/Bitbucket or an approved self-hosted provider; Leafrun reads and renders while the user pushes changes through Git.

Import creates an independent managed copy with history if it fits the plan. Stage privately, check size, then expose it. Upstream changes do not automatically overwrite the copy; pulling upstream updates and pushing externally require separate explicit actions.

Saving supplies `base_commit` and changed files. Serialize writes to the branch, recheck its tip and create the commit. A changed branch returns `409 source_conflict`; retain unsaved client text for comparison and merge. An operation ID ties Git, project metadata and audit together; reconciliation detects interruption between Git and PostgreSQL.

Direct user pushes to managed Gitea are outside the MVP. Every write passes Leafrun quotas; future Git-push support must enforce the same limits.

A connection belongs to a workspace and stores provider address, repository identity, mode and encrypted credentials. Server-controlled address rules apply. Tokens never enter URLs, project Git configuration or logs, and are not shared across workspaces. Cache identity includes workspace, connection, repository and access version.

Resolve ref to exact SHA before acknowledging admission, outside the database transaction. Preparation captures that commit and a service ref in a private retry copy. If force-push removes the commit before capture, return `source_revision_unavailable`; never substitute a new HEAD. Retried attempts use the pinned revision. Pins have retention, holder leases and separate disk budgets.

Keep pins through the overall deadline and active lease plus cleanup margin. After termination, remove the copy and private JSON under bounded cleanup policy. Store PDFs separately according to the historical plan policy.

Support ordinary Git files first. LFS, submodules and symlinks initially return explicit errors, matching the earlier engine. Full history, imports and temporary Git objects need limits independent of checkout size.

## 6. User and bot API

These were future public routes; `POST /api/render` was the working route in the earlier server revision, not in the current local-function checkout.

| Route | Purpose |
|---|---|
| `POST /v1/workspaces` | Personal or team workspace |
| `GET /v1/workspaces/{id}/projects` | Paginated projects |
| `POST /v1/workspaces/{id}/projects` | Managed template project or external connection |
| `GET /v1/projects/{id}/files` | Files at a specific commit |
| `POST /v1/projects/{id}/commits` | Save with `base_commit` |
| `POST /v1/projects/{id}/renders` | Admit render and capture SHA |
| `GET /v1/renders/{id}` | State, SHA, timing and authorized diagnostics |
| `GET /v1/renders/{id}/pdf` | Private PDF download |
| `POST /v1/renders/{id}/cancel` | Idempotent cancellation |
| `POST /v1/workspaces/{id}/api-keys` | Create scoped key |
| `DELETE /v1/api-keys/{id}` | Revoke key |
| `GET /v1/workspaces/{id}/usage` | Usage and remaining limits |
| `GET /v1/workspaces/{id}/audit` | Audit within role and plan |

```http
POST /v1/projects/{project_id}/renders
Authorization: Bearer <workspace_api_key>
Idempotency-Key: invoice-2026-0042
Content-Type: application/json
```

```json
{"ref":"main","entrypoint":"main.typ","data":{"title":"Invoice No. 42"}}
```

Return `202` with `render_id`, `state: queued`, `source_commit` and status/download paths. Input format defaults from the project. Store JSON privately, outside Git, until termination and the retry window end.

Idempotency is workspace-scoped: the same key and canonical request return the original job and SHA. Different project, ref or JSON gives `409 idempotency_conflict`. Database uniqueness enforces it; another compilation attempt is not another user job. Proposed key retention is seven days independent of PDF retention; tell clients when reuse becomes a new request.

| HTTP | Meaning |
|---|---|
| 400 | Invalid JSON, ref, path or field |
| 401 | Missing or invalid session/key |
| 403 | Known resource, insufficient action rights |
| 404 | Missing resource or outside authorized workspace |
| 409 | Git, idempotency or state conflict |
| 413 | Input, source or output too large |
| 429 | Quota, rate or queue capacity; include `Retry-After` when known |
| 502/503 | Git provider or admission service temporarily unavailable |

Compilation failure after admission appears as job state `failed`; it does not retroactively fail the completed POST. Status GET returns a successful HTTP response with machine error code and bounded diagnostics. Downloads require `succeeded` and a live artifact: expiry returns `410 artifact_expired`, unfinished work `409 artifact_not_ready`.

The first bot polls. Webhooks, schedules and Git-triggered rendering require separate event signatures, unique IDs, delayed retries and outbound-address protection. A signed Git event does not replace repository and permission checks.

## 7. Queue, retries, cancellation and publication

PostgreSQL stores job, input, SHA, reservation, attempt number, lease and state. [`SKIP LOCKED`](https://www.postgresql.org/docs/current/sql-select.html) supports claiming; fairness and leases require additional logic. Select a workspace with a free slot, then its oldest eligible job, preventing a large tenant occupying the entire queue.

Admission follows authentication → scope → existing key → preliminary limits → ref resolution → transaction that rechecks the key, atomically reserves quota, stores private input and creates queued state. Bounded JSON initially shares that transaction so workers never see a job without input. Git network calls do not hold database locks. Future object-backed inputs must upload first, then commit the reference with the job; collect unreferenced objects.

States: `queued`, `preparing`, `rendering`, `publishing`, `succeeded`, `failed`, `cancelling`, `canceled`, `expired`. PDF expiry changes availability, not historical render success; a queued job whose deadline expires becomes `expired`.

| Setting | Proposed MVP value |
|---|---|
| Attempt lease / heartbeat | 30 seconds / every 10 seconds |
| Queue wait | Up to 15 minutes |
| Attempt including preparation | Up to 60 seconds |
| Transient retries | At most 2 retries, 3 total attempts |
| Source / request body / PDF | 100 MiB / 1 MiB / 25 MiB |
| Temporary Git objects per preparation | Up to 250 MiB; interrupt on overflow |
| Overall processing deadline | Queue and all retries within 15 minutes of admission |

All values are configurable. Queue wait consumes no compiler. Retry only transient infrastructure errors; do not automatically retry Typst errors, limits, invalid sources or user cancellation.

Before execution, recheck deferred authorization, workspace suspension and reservations. Revoked keys or removed memberships cannot authorize new deferred work. A downgrade does not delete sources; restrict new writes/jobs until usage falls or the plan increases.

Every attempt has a fencing token. Publication requires the current lease, expected state and no effective cancellation. Execution may repeat, but user-job charging and publication happen once. Stale attempts cannot replace a newer result.

Write PDF bytes privately under `workspace/job/attempt`, then transactionally publish the reference, state, final charge and audit. Verify output reservation before publication. Cleanup removes unreferenced objects, expired Git pins and abandoned directories.

Cancelling queued work immediately refunds its reservation, as do queue expiry and failed authorization before preparation. Active work enters `cancelling`; stop its entire environment, wait for exit and clean files before marking `canceled`. Charging depends on whether preparation started, as described below. One transaction resolves cancellation versus publication. Cancelling a terminal job returns its state without repeating effects.

The manifest records SHA, JSON hash, entry point, format, adapter/template/Typst/worker-image/package/font versions. Arbitrary documents may depend on time or other allowed inputs, so byte-identical PDFs are not universally promised. Controlled templates pass dates explicitly and pin dependencies.

## 8. Execution and secrets

Trusted components are API, Git preparation, PostgreSQL, controller and artifact storage. The document runs in an isolated job environment with only its source and JSON; the controller remains outside, renews leases and reads output after termination.

The public-service proposal uses one container per job: unprivileged user, read-only root filesystem, bounded scratch disk, initial 512 MiB memory and one CPU, no added capabilities, syscall profile and whole-container timeout termination. Mount only the job directory. Exclude Docker socket, database credentials, Git tokens and global API key. Supply packages from a prepared cache or restricted proxy; arbitrary outbound access is disabled.

Choose the controller's execution adapter during implementation: a trusted controller with local rootless runtime on one dedicated host, then orchestrator jobs later. Compiler processes never receive runtime-management privileges. The earlier single rootless engine container is a starting point for a closed pilot; job isolation and negative tests precede public access.

Git proxy/import validates addresses, redirects and actual destination networks. External connections reject localhost, cloud metadata endpoints and internal networks; allow hosted Gitea separately by an exact server-defined address. User requests cannot supply arbitrary Git settings, hooks, filters or commands. Check project size before compilation and enforce disk limits while fetching objects.

Encrypt connection secrets with a key outside the database; hash passwords and random access keys. Logs omit secrets and document content. Typst diagnostics may contain source fragments, so store them separately as bounded private data with retention, not in general audit.

## 9. Plans, quotas and payment

These starting plans require measured render costs before prices or economics are approved. No plan means unlimited resources.

| Limit | Free | Pro | Team |
|---|---|---|---|
| Members | 1 | 1 | 10 |
| Projects, including external | 10 | 50 | 200 |
| Managed Git including history | 10 MiB | 100 MiB | 1 GiB |
| User jobs/month | 100 | 500 | 5,000 |
| Active workspace jobs | 1 | 2 | 4 |
| Queued workspace jobs | 10 | 25 | 50 |
| Private outputs | 50 MiB | 500 MiB | 5 GiB |
| PDF retention | 24 hours | 7 days | 30 days |
| Job history and audit | 30 days | 90 days | 365 days |
| Collaboration | Personal | Personal | Members, roles, Git history |

Company uses the same workspace mechanisms with negotiated settings; SSO, dedicated workers and special retention require separate estimates. External physical storage is not billed as managed Git, but download, service-copy and compute limits still apply.

Administrators initially assign plans. A future payment adapter verifies webhook signature, event uniqueness and transition validity and reconciles subscription state with the provider. A browser payment redirect alone grants nothing. Plans are server entitlements; payment handlers and public APIs cannot arbitrarily alter limits.

Reserve jobs at admission. Idempotent requests and internal retries do not charge again. User errors after preparation starts consume one job. Cancellation before preparation and terminal infrastructure failure refund it. Rate and aggregate network/disk limits prevent refunds enabling unlimited free attempts.

Measure Git storage after object creation and periodically reconcile it; concurrent writes share reservations. Check PDF usage before publication. Full output quota prompts deletion of older PDFs without deleting sources. Downgrades preserve reading/export while restricting new consumption.

## 10. Audit, diagnostics and metrics

Audit commits with application state; outbox supports reliable external delivery. Users cannot edit events. Owner/Admin see workspace audit; Editor/Viewer see authorized project history. Platform administrators use a separate audited privileged path.

Events include `account.login`, `membership.changed`, `api_key.created/revoked`, `project.created/imported`, `source.committed/conflict`, `render.accepted/canceled/succeeded/failed`, `quota.denied`, `plan.changed`, `artifact.downloaded/deleted`. Fields include UTC time, request ID, workspace, actor, object, SHA, outcome and error code. Exclude passwords, tokens, JSON, source and PDF bytes.

Technical logs contain process, phase, duration, error code and permitted IDs. Metrics include queue length/age, active jobs, p50/p95 duration, error ratio, CPU/memory, disk, Git latency, publication success and resource cost per workspace. Avoid per-user or per-SHA labels in general time series.

Operator log tools suffice for the pilot; searchable user audit is an application feature. Alert on missing workers, unavailable database/Gitea, growing queues, full disks and isolation failures. Operators configure thresholds and recipients at deployment.

## 11. Deployment, growth and recovery

Start with one Compose server for API, PostgreSQL, Gitea and private PDF volume; a trusted controller launches jobs on a dedicated execution runtime. One Gitea instance can retain SQLite; application PostgreSQL is separate. Persistent data uses named volumes and backups. Building images neither enables billing nor buys servers.

Initial global capacity is two concurrent executions and up to 50 queued jobs, with lower workspace limits. Reject overflow with 429. Validate per-job memory and global concurrency against host capacity.

Growth sequence: separate worker host → shared PDF/input storage → multiple API instances → automatic worker count. Local caches stay disposable. Git remains the source store; object storage supports outputs and private inputs, rather than replacing Gitea in this historical design.

Kubernetes [HPA](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/) scales workers from queue/resource metrics; [node autoscaling](https://kubernetes.io/docs/concepts/cluster-administration/node-autoscaling/) adds placement capacity. Operators define minimum, maximum, scale-down delay, budget and exhaustion behavior. Automatic infrastructure purchasing is disabled for demonstrations.

Gitea/PostgreSQL need independent capacity and availability strategies. Do not scale one SQLite-backed Gitea by adding processes. A shared network Git-cache directory does not replace synchronization and leases.

Back up application PostgreSQL, Git data, Gitea configuration, decryption keys and server access configuration. Pilot targets: daily off-server backups, RPO up to 24 hours and recovery drills targeting RTO up to four hours. These are verification goals, not an SLA. External repository owners retain responsibility for their repositories; a job pin is not a full external Git backup.

After restart, reclaim expired leases, verify published artifacts and resume cleanup without repeating cancellation or final charges. Users can export Git history and download live artifacts. Workspace deletion requires explicit product confirmation and bounded deletion of sources, keys and artifacts under the disclosed policy, including backup lifecycle.

## 12. Acceptance criteria

| Scenario | Criterion |
|---|---|
| Ordinary user | Register, create a template project, save and download PDF without manual Git |
| Markdown | Chosen examples cover links, images, lists, tables and Unicode; unsupported constructs are explained |
| Bot | Two identical keyed requests create one job and one charge |
| Other workspace | No access to its projects, JSON, diagnostics, PDFs, keys or audit |
| Editor conflict | Two saves with one base commit accept the first and explicitly reject the second |
| Changed Git | SHA stays fixed; force-push cannot substitute it; unavailable unpinned revisions fail |
| Load | Races cannot exceed quotas; one workspace cannot occupy all slots |
| Cancellation/failure | Processes terminate, leases recover, files are removed and stale results never publish |
| Plan/payment | Replayed webhooks cannot grant rights or charge twice |
| Recovery | A clean host restores accounts, projects, access and Git history from backup |
| Scaling | More workers reduce queue delay without exceeding node or spending limits |

The first complete implementation path is in [roadmap-v1.md](roadmap-v1.md). Public launch requires tenant and execution isolation, quotas and a tested restore. Simultaneous editing, external Git writes, SSO and automatic server provisioning were outside this historical first path.
