# Rendering reference

This page documents the current local Go wrapper. The short [README](../README.md) is the place to start.

## Function contract

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

Arguments are passed directly to `os/exec`, without a shell. The implementation is in [render.go](../render.go).

## Requirements

| Dependency | Requirement |
|---|---|
| Go | 1.27 or later, as declared in [go.mod](../go.mod) |
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


