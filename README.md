# Leafrun generation

Generation provides asynchronous server rendering and preserves the local Go
`Render` API from [Feruum/Leafrun](https://github.com/Feruum/Leafrun). E04's server
implementation uses the related Core, Contracts and Platform draft revisions;
these dependencies remain unreleased. See [the E04 service guide](docs/e04-service.md)
for roles, lifecycle, limits and verification.

## Server rendering

The [Infra E04 stand](https://github.com/leafrun/leafrun-infra) builds the API,
Temporal workers, isolated renderer and recovery processes with the pinned
dependencies. `POST /v1/jobs`, `GET /v1/jobs/{jobId}` and
`POST /v1/jobs/{jobId}/cancel` use Contracts `generation/v1`; successful jobs bind
an `artifactId` downloaded through Storage. Admission requires current access,
a scoped immutable snapshot and an idempotency key. JSON parameters are
normalized without changing numeric tokens, validated asynchronously against
Draft 2020-12 and saved as the derived snapshot's `input.json`.

## Quick start

Install [Go 1.27](https://go.dev/dl/) and [Typst 0.15.1](https://github.com/typst/typst/releases/tag/v0.15.1), with `typst` on your `PATH`. Other engine versions are rejected.

For this draft, use the complete source layout and startup instructions in the
related Infra repository. Configure that pinned workspace before the local example:

```powershell
. ../infra/scripts/test-env.ps1
go run ./examples/render ./examples/invoice
```

The command prints the absolute path to `examples/invoice/document.pdf`. Change [the invoice data](examples/invoice/data.json) and run it again. The example includes local JSON, an SVG, and a nested Typst file.

## Use from Go

```go
import leafrun "github.com/Feruum/Leafrun"

pdf, err := leafrun.Render("./project")
```

`Render(string) (string, error)` still uses `main.typ` and writes `<project>/document.pdf`. The wrapper explicitly passes that output path to the new core. Successful compilation atomically replaces the PDF; compilation and publication failures leave the previous PDF intact and return an empty path. Captured Typst diagnostics remain in the error text, and `errors.As(err, &exitError)` still exposes the original `*exec.ExitError`.

The Go module remains `github.com/Feruum/Leafrun`. `go get github.com/Feruum/Leafrun@latest` fetches the **original upstream module**, not changes made only in this fork. Clone this repository when working on the Leafrun version. Module path migration is separate work.

For a context, custom entrypoint/output, additional font paths, or structured diagnostics, use the [core API or CLI](https://github.com/leafrun/leafrun-document-core#go-api) directly. The wrapper enables `<project>/fonts` automatically.

Custom fonts previously supplied through `TYPST_FONT_PATHS` should move to `<project>/fonts` or the core's explicit font paths. The core ignores this inherited variable so every custom font directory is validated; embedded and system fonts remain enabled.

## Document projects

Place `main.typ` and its resources under a local project directory. Relative imports and resources follow Typst's own semantics, with an explicit project root. Leafrun rejects entrypoints and project links that escape that root. See [the rendering reference](docs/rendering-reference.md) for paths, fonts, diagnostics, and limits.

The local wrapper's caller prepares source files and keeps files and directory
links unchanged during compilation. Server jobs instead read captured immutable
snapshots through Storage; real Access and collaboration remain later stages.

## Development

```powershell
. ../infra/scripts/e04-test-env.ps1
go test ./... -count=1 -v
go vet ./...
go build ./...
go run ./examples/render ./examples/invoice
```

Typst integration tests require version 0.15.1. Server PostgreSQL tests require
the isolated `GENERATION_TEST_DATABASE_URL`; use Infra's `e04-test-env.ps1` in the
development workspace. CI checks the pinned source workspace on Linux, Windows
and macOS. The Infra workflow additionally runs actual PG/S3/Temporal/NATS,
renderer crash/restart and Linux race checks. See the
[M01 verification record](docs/verification.md) for the earlier wrapper checks.

No Leafrun project license has been selected yet.

Contributions use English and git flow; see [CONTRIBUTING.md](CONTRIBUTING.md) for feature, integration, and release branches.
