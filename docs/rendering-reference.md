# Rendering reference

The compatibility wrapper delegates to [Leafrun document core v0.1.0](https://github.com/leafrun/leafrun-document-core). Start with [the README](../README.md).

## Function contract

```go
func Render(repoPath string) (string, error)
```

| Item | Behavior |
|---|---|
| Project | Local directory, absolute or relative to the caller's working directory |
| Entrypoint | `<project>/main.typ` |
| Output | `<project>/document.pdf`, explicitly selected by the wrapper |
| Success | Absolute PDF path and a nil error |
| Failure | Empty path and an error; any previous PDF remains intact |
| Execution | Synchronous; waits for the engine and publication |

The wrapper uses `context.Background()`. For cancellation, custom paths, extra font directories, or structured diagnostics, use the core's `Render(ctx, RenderOptions)` directly. The module path remains `github.com/Feruum/Leafrun`; it has not been migrated.

## Requirements

Go 1.27.0 and the exact Typst CLI version **0.15.1** are required. Install the appropriate [official release archive](https://github.com/typst/typst/releases/tag/v0.15.1), extract it, and put the binary's directory on `PATH`. The core rejects other versions before compiling.

```sh
go version
typst --version
go mod download
```

Rendering requires readable project files and a writable output directory. No Git directory or remote service is needed. The Go dependency is pinned in [go.mod](../go.mod).

## Prepare a document project

```text
project/
├── main.typ
├── data.json
├── assets/logo.svg
├── sections/details.typ
└── fonts/FiraSans-Regular.ttf
```

```typst
#let data = json("/data.json")
= #data.title
#image("/assets/logo.svg", width: 12mm)
#include "sections/details.typ"
```

Typst paths starting with `/` refer to the explicit project root. Relative imports and resources resolve from the file using them. The caller writes data files before rendering; the wrapper takes no separate JSON input.

Project entrypoints and symlinks/junctions must stay within the project. The core checks links before starting the engine, and Typst's root rules reject source traversal such as `../secret.txt`. M01 assumes sources and links do not change during compilation.

### Fonts and packages

`<project>/fonts` is automatically added when present. To add other font directories, use the core API's `FontPaths` or repeat the core CLI's `--font-path`. Font-directory links must stay within their allowed directory.

The core ignores inherited `TYPST_FONT_PATHS`. Move custom fonts supplied through that variable to the project font directory or explicit options. Typst's embedded and system fonts are unaffected.

Typst still provides embedded and system fonts and its package-import behavior. Registry packages may require a first download into Typst's package cache. The included invoice uses local resources and needs no package download. See [Typst packages](https://github.com/typst/packages).

## Diagnostics and output integrity

Warnings can accompany a successful PDF. Syntax errors, unknown names, missing resources, `assert`, and `panic` are ordinary compilation failures. Typst's full human diagnostics remain in the returned error text.

The core wraps failures in `*documentcore.RenderError` and preserves the original cause:

```go
var exitError *exec.ExitError
if errors.As(err, &exitError) {
    // The original Typst process failure is available.
}
```

The core result also exposes normalized severity, message, hints, ordered trace, and nullable source positions. It preserves emitted paths/line/column and raw text without inventing ranges or offsets.

Compilation uses a unique temporary file beside the output. Only a completed, validated PDF is atomically published; Windows has a separate replacement implementation. Failure, cancellation, or a locked destination preserves the previous PDF. A PDF already on disk is not evidence that the latest render succeeded: use the returned error.

## Scope and limits

The wrapper retains its fixed entrypoint/output and has no context parameter. Concurrent callers using the same directory target the same `document.pdf`; serialize those calls or use the core with distinct outputs. Neither package snapshots a mutable source tree.

Successful compilation does not judge layout or content quality. An oversized page element can compile successfully; the user reviews the resulting document. HTTP endpoints, Git downloads, jobs, storage, access control, resource limits, and WASM are later stages.

## Troubleshooting

| Symptom | Check |
|---|---|
| Typst cannot be found or has the wrong version | Run `typst --version` in the same environment as the Go program; install 0.15.1 |
| `main.typ` is missing | Pass a local project directory containing that file |
| Import, image, or data file is missing | Check its path and keep local resources under the project root |
| Font warning | Put the font under `<project>/fonts` or supply a core font path |
| Package cannot be downloaded | Check the package name/version and network access |
| Output cannot be published | Check directory permissions and PDF-viewer locks |
| Error but an existing PDF remains | The old PDF is deliberately preserved; fix the diagnostic and render again |

[Verification record](verification.md) documents the wrapper and core checks.


