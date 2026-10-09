<p align="center">
  <img src="docs/assets/leafrun-logo.png" width="112" height="112" alt="Leafrun logo">
</p>

<h1 align="center">Leafrun</h1>

<p align="center">
  <strong>Typst projects in. PDFs out.</strong><br>
  A tiny Go package for rendering local document projects.
</p>

<p align="center">
  <img src="docs/assets/runtime-badges.svg" width="232" height="24" alt="Go 1.27 or later · Typst CLI 0.15.1">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> &nbsp;·&nbsp;
  <a href="#use-from-go">Go API</a> &nbsp;·&nbsp;
  <a href="#prepare-a-document-project">Project files</a> &nbsp;·&nbsp;
  <a href="#development">Contributing</a>
</p>

Give Leafrun a folder containing `main.typ`. It runs the [Typst](https://typst.app/) CLI, writes `document.pdf`, and returns the absolute output path:

```go
pdf, err := leafrun.Render("./project")
```

Use it for invoices, reports, letters, and other documents with local JSON data and assets. Typst handles the typesetting; your application prepares the files and delivers the PDF.

## Quick start

With Go and Typst installed, clone Leafrun and render the included invoice. See [requirements](#requirements) for installation details.

```sh
git clone https://github.com/Feruum/Leafrun.git
cd Leafrun
go run ./examples/render ./examples/invoice
```

The command prints the absolute path to `examples/invoice/document.pdf`. Open that file in a PDF viewer.

The example uses JSON data, an SVG image, and an included Typst file. Edit [examples/invoice/data.json](examples/invoice/data.json) and run the same command again to regenerate the invoice. The sample document's text is in Russian; you can change its text and data in the project files.

To render another local project from this checkout:

```sh
go run ./examples/render "/absolute/path/to/document-project"
```

Relative paths also work. Quote paths containing spaces. The example command accepts exactly one directory argument, prints the output path on success, and exits with an error if compilation fails.

## Use from Go

In your own Go module, add the package:

```sh
go get github.com/Feruum/Leafrun@latest
```

Then call `Render`:

```go
package main

import (
	"fmt"
	"log"

	"github.com/Feruum/Leafrun"
)

func main() {
	pdf, err := leafrun.Render("./project")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pdf)
}
```

### Function contract

The public API is one function:

```go
func Render(repoPath string) (string, error)
```

| Item | Behavior |
|---|---|
| Input | A local directory path, absolute or relative to the calling process's working directory |
| Entry point | `<project>/main.typ` |
| Output | `<project>/document.pdf`; an existing output is overwritten on a successful render |
| Success | Absolute PDF path and `nil` error |
| Failure | Empty path and an error; Typst command failures include captured diagnostics |
| Execution | Synchronous: the call waits for the Typst process to finish |

The function resolves the project path, sets it as both the process working directory and Typst project root, and executes the equivalent of:

```sh
typst compile --root <project> <project>/main.typ <project>/document.pdf
```

Arguments are passed directly to `os/exec`, without a shell. The implementation is in [render.go](render.go).

## Requirements

| Dependency | Requirement |
|---|---|
| Go | 1.27 or later, as declared in [go.mod](go.mod) |
| Typst CLI | Installed and available as `typst` in `PATH`; verified with 0.15.1 |
| Project directory | Readable local files, a `main.typ` entry point, and permission to write `document.pdf` |

Leafrun has no third-party Go dependencies. Git is only needed to clone this repository or to prepare a Git-backed document project; rendering itself does not require a `.git` directory.

Install Go from [go.dev](https://go.dev/dl/). Install Typst using its [official installation instructions](https://typst.app/open-source/#install-the-compiler), or download a binary for your system from the [0.15.1 release](https://github.com/typst/typst/releases/tag/v0.15.1) and add its directory to `PATH`.

Verify the tools before running Leafrun:

```sh
go version
typst --version
```

## Prepare a document project

A minimal project needs one file:

```text
project/
└── main.typ
```

Put this in `main.typ`:

```typst
= Hello, Leafrun

This PDF was generated from a local Typst project.
```

For a document with data and images, a project can look like this:

```text
project/
├── main.typ
├── data.json
├── assets/
│   └── logo.svg
└── sections/
    └── details.typ
```

Typst files read these resources directly. For example:

```typst
#let data = json("/data.json")
= #data.title
#image("/assets/logo.svg", width: 12mm)
#include "sections/details.typ"
```

Here, paths starting with `/` refer to the project root passed to Typst. Relative resource paths are resolved by Typst relative to the file that uses them.

Leafrun takes no JSON argument and creates no data files. Your application writes or updates `data.json` before calling `Render`. If the source lives in Git, clone or check out the desired revision before calling the function.

### Fonts and packages

Typst handles fonts and package imports. Leafrun inherits the calling process's environment. To use project-specific font files, set Typst's `TYPST_FONT_PATHS` to an absolute directory path before running your application:

```sh
# Linux / macOS
export TYPST_FONT_PATHS="/absolute/path/to/project/fonts"
```

```powershell
# Windows PowerShell
$env:TYPST_FONT_PATHS = 'C:\path\to\project\fonts'
```

Putting a `fonts/` folder in a project does not make Leafrun configure it automatically. For multiple font directories, Typst uses the system path separator: `:` on Linux/macOS and `;` on Windows.

Typst can download imported registry packages into its own package cache, so the first render of a project using packages may need internet access. This cache belongs to Typst; Leafrun does not maintain a project or PDF cache. See [Typst's package documentation](https://github.com/typst/packages).

## Current scope and limitations

The current package deliberately exposes only local compilation:

- The entry point is always `main.typ`, and the output is always `document.pdf` in the same project directory.
- Each call compiles a new PDF. The caller owns the source files and the output file, including any cleanup.
- Calls targeting the same project directory share one output path. Serialize them or prepare a separate directory for each job. Leafrun provides no locks or project snapshots.
- There is no context parameter, cancellation mechanism, timeout, concurrency limit, or resource limit in the wrapper. It relies on the Typst process to finish.
- HTTP endpoints, Git downloads, credentials, queues, accounts, collaboration, billing, and automatic scaling are outside the current implementation.
- Sources must use Typst syntax. Markdown requires a separate conversion step or a compatible template/package.

Typst supplies its own file-access rules through the project root. Leafrun adds no container or operating-system isolation. If you build a public service around it, handling user access, preparing isolated working directories, and enforcing resource limits belong to that service.

## Troubleshooting

| Symptom | What to check |
|---|---|
| `typst` cannot be found | Run `typst --version` in the same environment as the Go program and add the CLI directory to `PATH` |
| `main.typ` cannot be read | Pass the project directory, rather than a file path or a remote Git URL, and check that `main.typ` exists |
| Image, JSON, or include is missing | Check the resource path and keep the required files inside the Typst project root |
| A font is unavailable | Install it or set an absolute `TYPST_FONT_PATHS` before starting the program |
| A package cannot be downloaded | Check internet access and the package name/version reported by Typst |
| The output cannot be written | Check directory permissions and whether a PDF viewer has locked `document.pdf` |
| Rendering returns an error but a PDF is present | Use the returned error; an older output file may still exist and is not evidence of a successful new render |

Typst syntax errors are returned with the CLI's diagnostic output. Read that error to locate the failing file and line.

## Development

With Go and Typst installed:

```sh
go test ./... -count=1 -v
go vet ./...
go run ./examples/render ./examples/invoice
```

The tests execute the real Typst CLI and verify a PDF built from local JSON, an image, and an included file; a relative project path containing spaces; compiler diagnostics; and a missing entry point. **They skip when Typst is not in `PATH`**, so make sure the test output shows a pass rather than a skip.

The current implementation has been checked on Windows with Go 1.27.0 and Typst 0.15.1. Go uses portable standard-library APIs, but this repository does not claim a verified operating-system matrix.

For an existing Windows checkout that already has the downloaded CLI under `.tools`, add it to the current PowerShell session:

```powershell
$env:PATH = (Resolve-Path '.tools/typst-v0.15.1/typst-x86_64-pc-windows-msvc').Path + [IO.Path]::PathSeparator + $env:PATH
```

The `.tools` directory is local and ignored by Git; it is not supplied by a fresh clone.

### Repository layout

```text
render.go                    Public Render function
render_test.go               Tests using the real Typst CLI
examples/render/main.go      Command-line example
examples/invoice/            Example document, data, and assets
docs/assets/                 Leafrun logo and its generation prompt
docs/architecture/           Platform design and diagrams
docs/superpowers/            Earlier implementation plans and verification records
```

The [Leafrun logo](docs/assets/leafrun-logo.png) is a transparent PNG created with imagegen. Its [generation prompts](docs/assets/leafrun-logo.prompt.md) are saved alongside the asset. The runtime badges are local SVGs, so the header needs no external image service.

Questions and contributions can be discussed through [GitHub issues](https://github.com/Feruum/Leafrun/issues) and pull requests. Include a minimal document project and the Go/Typst versions when reporting a rendering problem. Keep changes consistent with the small public API; discuss larger additions before implementation.

## Design and project history

The repository was simplified to this local function on **October 9, 2026**. The previous synchronous HTTP renderer, Git cache, Gitea integration, and Docker/Compose setup remain in Git history at [commit d77d9a9](https://github.com/Feruum/Leafrun/tree/d77d9a9bf53f64371820a6b1b84e693a3dcd6486).

The following documents are retained for future development and are currently written in Russian:

- [Architecture and product design](docs/architecture/leafrun-saas-v1.md)
- [Diagram index and current local rendering flow](docs/architecture/board-index.md)
- [Development roadmap](docs/architecture/roadmap-v1.md)
- [Miro board](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686553812306)

They describe a possible platform around the rendering core. Historical readiness checklists refer to the earlier server implementation; the current checkout contains the local function described above.

## License status

A license has not been selected yet, and no `LICENSE` file has been added. The repository does not currently declare MIT, Apache-2.0, or another distribution license.

Typst is a separate dependency with its own [license](https://github.com/typst/typst/blob/main/LICENSE).
