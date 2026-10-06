# Verification: Typst rendering engine

Completed on 2026-10-06. Runtime: Go 1.27.0, Typst 0.15.1 (9dfd3a08), Windows Git 2.47.1; Linux Docker Engine 29.1.2. Container tests use Alpine Git 2.54.0 and Gitea 28.0.0 rootless.

## Automated checks

| Check | Result |
| --- | --- |
| Native `go test ./... -count=1` with the pinned real `TYPST_BIN` | PASS, final production source, 86.788 seconds |
| Native `go vet -buildvcs=false ./...` | PASS |
| Native `go build -buildvcs=false -o bin/renderd.exe ./cmd/renderd` | PASS |
| Docker `test` target: `go test -race ./... -count=1` and `go vet ./...` | PASS |
| Complete Linux race suite with the final test source mounted read-only | PASS, 19.327 seconds |
| Project-font test explicitly executed on Windows and Linux | PASS, Arial and DejaVu Sans respectively |
| Official `@preview/tablex:0.0.9` package download into a request-local cache | PASS |
| Production Docker build and Compose configuration | PASS |

The native tool environment has different Git ownership from the repository owner. Native vet/build therefore disabled VCS stamping per invocation; no global Git configuration was changed. The Windows host has no C compiler on PATH, so race detection ran in Linux with GCC/musl.

Tests exercise real local Git repositories and a CGI smart-HTTP Git server. Coverage includes cache reuse, new commits, force-push, pruned branch/tag deletion, remote HEAD changes, ambiguous refs, reachable historical SHAs and rejection of stale unreachable SHAs, fetch failure without stale fallback, parallel version snapshots and JSON isolation, authentication, admission limits, unsupported Git features, paths and both blob/checkout size limits.

Real Typst tests exercise Unicode JSON, nested entrypoints/includes, SVG images, project fonts, official packages, compiler diagnostics, timeouts and worktree cleanup. A Linux subprocess test checks that cancellation kills descendants.

Regression tests first reproduced, then verified fixes for concurrent cleanup returning before removal, expanded checkout files exceeding the blob limit, abandoned Git transaction locks, leaked cache map identities, unreclaimed force-pushed objects, a real socket timeout returning EOF, and buffered compiler errors delayed behind another repository operation. Error and successful PDF responses now flush before deferred cleanup; JSON has an explicit content length.

## Compose end-to-end checks

An isolated `typst-render-check` deployment used API port 18080 and Gitea port 13000, with separate named volumes. Its generated credentials were stored only in ignored `.cache/` files.

1. Started unmodified Gitea Compose configuration and completed its SQLite installation. The installer health endpoint allowed the renderer dependency to start.
2. Created a private `invoice` repository, uploaded the shipped example, created a separate reader user, assigned repository Read access and generated a `read:repository` token.
3. Rendered the example through `POST /api/render`; verified HTTP 200, PDF content/type, actual source SHA, non-root UID 10001 and empty request directory after transfer. The first PDF was 15,133 bytes.
4. Pushed a new commit; the next render used its new SHA, `2ad8ffd54e4e27ecdd7f0cae518b4ab9ad794b4d`.
5. Recreated the renderer without Git credentials; the private repository returned `502 git_error`.
6. Restarted Gitea and recreated the renderer with configured read credentials. The Gitea repository persisted, the renderer lost its disposable tmpfs cache, and the next render succeeded from a newly downloaded project.
7. Imported `https://github.com/octocat/Hello-World.git` through Gitea's standard migration API with mirroring disabled; the resulting repository preserved three historical commits.
8. Built the final production source and rendered the supplied request example against the private project. The downloaded demonstration PDF contains 15,424 bytes at `.cache/document.pdf`. No request worktree remained.

The independent read-only reviewer inspected the engine, lifecycle fixes, Dockerfile, Compose, documentation and examples. All important actionable findings were repaired and re-reviewed. The source and development tools are retained locally; the isolated test containers and their disposable test volumes are removed after verification.

## Delivery scope

One renderer instance with standard-library Go code, a pinned Typst CLI, Gitea/SQLite deployment, configuration example, documented API/errors/default limits, and a sample Typst project. LFS, submodules, symlinks and private external Git remain intentionally unsupported in v1. ARM64 packaging is defined with a pinned Typst checksum; execution was tested on Linux amd64 and Windows amd64.
