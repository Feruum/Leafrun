# Typst rendering engine design

Approved on 2026-10-06. Build a Go standard-library service and self-hosted Gitea, with one synchronous `POST /api/render` endpoint.

## Contract

The request accepts required `repo_url`, optional `ref` (remote HEAD by default), `entrypoint` (`main.typ` by default), and JSON object `data` (`{}` by default). Refs support branches, tags, and full reachable commit IDs; ambiguous branch/tag names require qualified refs. Authenticate callers with a configured bearer API key. Return a downloadable PDF and `X-Source-Commit`, or JSON with a stable error code, message, and bounded diagnostics.

## Pipeline and storage

Internal stages are compare, download, render, and upload. Maintain a disposable bare Git cache per repository. Every render performs a successful forced/pruning fetch and resolves remote HEAD or the requested ref to an immutable commit. Handle rewritten/deleted branches and changes to remote HEAD; a failed freshness check fails the request. Only current remote refs establish reachability for SHA requests.

Serialize Git cache operations per repository, create a separate detached worktree per request, and render outside the cache lock. JSON input belongs only to that worktree; expose its generated path through `sys.inputs.data_file`. Compile with Typst CLI 0.15.1 and an explicit project root. Output goes outside the source tree. Stream the completed PDF, then remove its worktree and request directory. Cancellation/timeout kills and waits for subprocesses before cleanup. Startup recovers abandoned worktrees. Idle caches expire after 24 hours and active leases prevent eviction. PDFs are not cached.

External Git is public. Private projects are supported only on the configured Gitea, using server-side read-only Git credentials. Rewrite the configured public Gitea URL to its internal Compose URL. Credentials never appear in URLs, returned diagnostics, or logs. Validate network URLs and entrypoint containment; reject symlinks, submodules and Git LFS in v1. Support ordinary images, nested Typst sources, project fonts in `fonts/`, and official Typst packages.

## Deployment and defaults

One renderer instance, two concurrent requests, 60-second request deadline, 1 MiB JSON request body, and 100 MiB checked-out project; all configurable. A Go Docker image contains Git and pinned Typst. Compose starts the API on 8080 and `docker.gitea.com/gitea:28.0.0-rootless` on 3000, using SQLite and named data/config volumes. Gitea handles external repository migration, editing, branches, commits, and merges. The renderer owns no project-management API.

Deliver source, Dockerfile, Compose, example environment, a sample Typst project, and instructions for initial Gitea setup, import, private read access, render, and PDF download. Verify real Git version changes, cache reuse/recovery, concurrent isolation, input rendering, failure cleanup, and credential confinement. Container execution requires a working Docker daemon.
