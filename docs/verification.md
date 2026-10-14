# M01 compatibility verification

Verified locally on **2026-10-11**, Windows amd64, **Go 1.27.0**, **Typst 0.15.1 (9dfd3a08)**, with the proposed document-core implementation in a local development workspace.

- `go test ./... -count=1 -v`: passed, no skipped tests. Original resource-render, process-error, and missing-entrypoint tests passed.
- `TestVersionMismatchPreservesExistingResult`: the old wrapper accepted a helper reporting Typst 0.15.0 (observed failure before the change); the core-backed wrapper rejects it, returns no path, and preserves the previous PDF.
- `TestRenderPreservesAbsoluteProjectAliasPath`: the wrapper preserves the original absolute caller path through a project junction/symlink, even when the core resolves the physical directory.
- `go vet ./...` and `go build ./...`: passed.
- `go run ./examples/render ./examples/invoice`: produced `examples/invoice/document.pdf` successfully.

The module path remains `github.com/Feruum/Leafrun`. The wrapper explicitly selects `main.typ` and `document.pdf`, and `errors.As` continues to expose the original `*exec.ExitError`.

The real core test matrix and its fixture distinctions are recorded in [document-core verification](https://github.com/leafrun/leafrun-document-core/blob/main/docs/verification.md).

## Release dependency

This change targets `github.com/leafrun/leafrun-document-core v0.1.0`. The core PR must be accepted and that version published before this PR is merged. After release, run `GOWORK=off go mod tidy`, commit the resulting `go.sum`, and run the listed checks from a clean checkout. The current local result does **not** claim verification against an already published version.

CI installs the pinned Typst through the core's `v0.1.0` installer and tests natively on Linux, Windows, and macOS; Linux additionally runs the race detector. M01 assumes source files remain unchanged during rendering.
