# Typst Renderer Implementation Plan

> **For agentic workers:** Use executing-plans to implement this plan task by task, with test-driven development and a final code review.

**Goal:** Deliver a synchronous Git-to-PDF API and a self-hosted Gitea deployment.

**Architecture:** Go owns request validation, disposable Git caches, detached request worktrees, subprocess lifecycle, and PDF transfer. Gitea persists editable projects. Typst CLI performs document compilation.

**Tech Stack:** Go 1.27 standard library, Git CLI, Typst CLI 0.15.1, Gitea 28.0.0 rootless, SQLite, Docker Compose.

## 1. Configuration and request contract

- [ ] Add `internal/renderer/config_test.go` with environment defaults, invalid limits, mandatory API key, Gitea URL mapping, and external credential rejection tests; run `go test ./internal/renderer` and observe missing implementation.
- [ ] Implement configuration and typed errors in `internal/renderer/config.go` and `errors.go`. Test key defaults: 60 seconds, concurrency 2, body 1048576 bytes, project 104857600 bytes, cache TTL 24 hours. Restrict transports to HTTPS plus the configured internal Gitea HTTP endpoint; allow configured Git hosts only.
- [ ] Run the focused tests and `go vet ./...` after the HTTP entrypoint is present.

## 2. Git cache and process lifecycle

- [ ] Add real-Git integration tests in `internal/renderer/cache_test.go`: first checkout, new commit, force-push, deleted refs, branch/tag ambiguity, moved remote HEAD, full-SHA reachability, stale fetch failure, simultaneous snapshots, expiration, abandoned checkout recovery, LFS/submodule/path limits. Use temporary local repositories internally; production URLs remain validated by the API layer.
- [ ] Implement `process.go` and platform process helpers: argument arrays, bounded diagnostics, clean Git environment, Linux process-group cancellation, Windows process-tree cancellation, and waits before cleanup.
- [ ] Implement `cache.go`: bare initialization, atomic forced/pruning branch/tag fetch, refreshed remote HEAD, ref resolution, unique detached worktrees, leases, cleanup/recovery, and idle eviction. Reject unsupported project features before compilation.
- [ ] Run `go test ./internal/renderer -run TestCache -count=1`; inspect failures before making changes.

## 3. Compiler and HTTP service

- [ ] Add `compiler_test.go` and `server_test.go`: exact real PDF rendering when Typst is available, JSON/Unicode input, nested includes/image/project font/package support, authentication, invalid paths/URLs/data, busy service, deadline, error diagnostics, and cancellation cleanup. Real-Git fixtures exercise the complete handler; subprocess helper tests cover compiler failures without external dependencies.
- [ ] Implement `compiler.go` and `server.go`: one authenticated endpoint, strict bounded JSON decoding, per-request deadline and semaphore, compare/download/render/upload pipeline, project-relative diagnostics and secret redaction, correct PDF headers and streaming cleanup. Pass JSON via a random project input file and `sys.inputs.data_file`.
- [ ] Add `cmd/renderd/main.go`: load configuration, check runtime dependencies, recover cache, start periodic eviction, serve HTTP, and gracefully drain requests on shutdown. Run `go test ./...` and build `go build -o bin/renderd.exe ./cmd/renderd`.

## 4. Packaging and documentation

- [ ] Add a multi-stage `Dockerfile` with the pinned Linux Typst release verified against its SHA-256 digest, Git, non-root execution, and the compiled Go binary. Add `compose.yaml` with loopback API/Gitea ports, persistent Gitea volumes, SQLite, registration/SSH disabled, and configuration via `.env.example`.
- [ ] Add `examples/invoice/main.typ`, nested content, SVG image, and input JSON demonstrating `json(sys.inputs.data_file)`. Document standard embedded fonts, `fonts/`, official package fetching, and unsupported LFS/submodules.
- [ ] Write `README.md` with startup, initial Gitea installation/account setup, public Git migration without mirroring, read-only service account token setup, curl/PowerShell PDF download, errors/limits/cache semantics, and verification commands.

## 5. Verification and review

- [ ] Download the exact Typst runtime into ignored `.tools/`, verify the release checksum, and run real-PDF integration tests with `TYPST_BIN` pointing to it.
- [ ] Run `go test ./... -count=1`, `go test -race ./... -count=1` when the host toolchain supports race builds, `go vet ./...`, native and Linux builds, and `docker compose config` with isolated test environment values. Attempt container startup only with a working Docker daemon.
- [ ] Request an independent code review against the approved spec, repair important findings, rerun affected checks, and record the final verification and any environment limitation.
