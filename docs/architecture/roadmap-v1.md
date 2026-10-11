# Leafrun: historical SaaS implementation stages

> Historical roadmap, retained from upstream. Superseded by the [current 16-milestone roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md) and [SOA architecture](https://github.com/leafrun/leafrun/blob/main/docs/architecture.md). As of October 9, 2026, this checkout contains a local Go rendering function. Checked items in stage 0 refer to the earlier server at commit `d77d9a9`.

October 7, 2026. Decomposition of the [v1 architecture](leafrun-saas-v1.md). Checkboxes record that historical revision; unchecked items were future work.

## Stage 0. PDF engine

- [x] Go API accepts a Git project, ref, entry point and JSON and returns PDF and SHA.
- [x] Cache, separate worktrees, limits, cancellation and cleanup checked.
- [x] Hosted Gitea, private reads, history import and persistence checked.
- [x] Docker, Typst example, images, fonts, packages and instructions delivered.

Evidence: [October 6 verification](../superpowers/verification-2026-10-06.md). This supplies the compilation foundation; the new SaaS interface was not implemented.

## Stage 1. First complete user workflow

Goal: one person signs in, creates a project, saves a document, renders and downloads a PDF.

- [ ] PostgreSQL: users, sessions, workspaces, memberships, projects, Git connections, render jobs, attempts, inputs, artifacts, reservations, audit and outbox.
- [ ] Registration, verified email, login/logout and recovery; attempt limits, cookie and CSRF protection.
- [ ] Personal workspace and verified access context on every path.
- [ ] Private managed repository creation from a fixed template, write quota and recoverable incomplete operations.
- [ ] Project page, file tree and editor with explicit save; Git commit with `base_commit` and conflict response.
- [ ] PostgreSQL queue, SHA pins, lease/heartbeat, attempts, cancellation and private results.
- [ ] Trusted controller and isolated environment per job; adapter around existing compiler code.
- [ ] Render history, error codes, PDF panel and action audit.
- [ ] Verified Markdown adapter and separate Typst template; Unicode, image and unsupported-construct tests.

Acceptance: complete the workflow in the browser; two workspaces cannot see one another's data; failed or cancelled work leaves no running execution. The saved SHA matches the manifest and PDF. Users edit without manually operating Git.

## Stage 2. Bots, external Git and limited beta

- [ ] Bot keys with scopes, projects, expiry and revocation; owner rights checked at request and deferred execution.
- [ ] Public `/v1` contract, client examples, pagination, idempotency and documented errors.
- [ ] Read-only external connections, server address rules, isolated source preparation and encrypted credentials.
- [ ] Independent managed imports preserving external history and checking size.
- [ ] Atomic quotas, monthly reservations, output/queue limits, fairness and request rate limits.
- [ ] Project export, output deletion, retention policy and orphan cleanup.
- [ ] Negative tests for leases, SSRF, cross-tenant access, disk/memory overflow and cancellation/publication races.
- [ ] Backup and restore on a clean environment.

Acceptance: an idempotent bot request does not charge twice; it renders an authorized external project and downloads only authorized results. Disconnected Git returns a clear error. Beta has bounded users and a global resource budget.

## Stage 3. Teams and paid features

- [ ] Invitations, roles, multiple participants and company ownership.
- [ ] Exactly one Owner, atomic ownership transfer and member-capacity reservation on invitation acceptance.
- [ ] Role changes, key revocation and user removal cannot leave newly authorized work in the queue.
- [ ] Server-side plan entitlements, usage display and downgrade behavior.
- [ ] Manual plan assignment, then payment adapter with signatures and event deduplication.
- [ ] Team audit, filtering and export within role/plan.
- [ ] Shared Git saves: conflicts, comparison and authorship history.

Acceptance: invitees receive exactly their roles; quotas are workspace-wide; repeated payment events cannot apply rights twice. Determine costs and prices from measurements.

## Stage 4. Growth and operations

- [ ] Queue, render, error, disk and resource metrics; operator thresholds and alerts.
- [ ] Multiple execution nodes and shared private input/output storage.
- [ ] Multiple API instances, bounded database connections and process-failure tests.
- [ ] Worker scaling from queue/load signals, separately from server capacity.
- [ ] Minimum/maximum, stabilization, budget and exhaustion behavior.
- [ ] Regular recovery drills and explicit selection of stricter availability goals.

Acceptance: additional workers reduce wait without changing source revision or isolation. Node loss permits retry but only one publication. Execution stays within budget even during free-request spikes.

## Later directions

Simultaneous editing, SSO/OIDC, per-project human permissions, external Git writes, webhooks/schedules, dedicated enterprise environments, preview history and a visual template editor require separate specifications. They must not block this historical first complete workflow.

The next task at the time was [PostgreSQL foundation and workspace isolation](../superpowers/plans/2026-10-07-workspace-foundation.md), followed by accounts/personal workspaces and an integrated create/save/queue path. This document describes order and acceptance, not completed SaaS code.
