# Leafrun generation

Render a local Typst project from Go. This working prototype is a [fork of Feruum/Leafrun](https://github.com/Feruum/Leafrun); its Git history is preserved.

## Quick start

Install [Go 1.27](https://go.dev/dl/) and [Typst 0.15.1](https://github.com/typst/typst/releases/tag/v0.15.1), with `typst` on your `PATH`. Then run the included invoice:

```sh
git clone https://github.com/leafrun/leafrun-generation.git
cd leafrun-generation
go run ./examples/render ./examples/invoice
```

The command prints the absolute path to `examples/invoice/document.pdf`. Change [the invoice data](examples/invoice/data.json) and run it again to replace the PDF. A project needs `main.typ` and any files it references; the example also includes JSON, an SVG, and another Typst file.

## Use from Go

```go
import leafrun "github.com/Feruum/Leafrun"

pdf, err := leafrun.Render("./project")
```

`Render` accepts a local folder and returns the absolute path to `<project>/document.pdf` or a Typst error. It waits for compilation to finish. An existing output is overwritten after a successful render. See the [rendering reference](docs/rendering-reference.md) for file paths, fonts, packages, errors, and limits.

The Go module is still `github.com/Feruum/Leafrun`. `go get github.com/Feruum/Leafrun@latest` fetches the **original upstream module**, not changes made only in this fork. Clone this repository when working on the Leafrun version. Changing the module path is separate future work.

## Requirements

Go 1.27 and a `typst` command on `PATH` are required. The wrapper has no third-party Go dependencies. It does not download a project or run a server. To render a Git project today, first check it out into a local folder. See [requirements and limitations](docs/rendering-reference.md#requirements).

## Prepare a document project

Put a `main.typ` in a folder and pass the folder path to `Render` or `examples/render`. Additional Typst files, data, and images can live under the same folder. See [file examples](docs/rendering-reference.md#prepare-a-document-project).

## Development

```sh
go test ./... -count=1 -v
go vet ./...
```

The tests call the real Typst CLI and can skip if it is missing, so check the test output. For a rendering bug, include a small project and your Go and Typst versions in a [GitHub issue](https://github.com/leafrun/leafrun-generation/issues).

Server jobs, PDF storage, access control, and collaboration are planned elsewhere in [Leafrun](https://github.com/leafrun/leafrun). Follow its [roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md) or [contribution guide](https://github.com/leafrun/leafrun/blob/main/CONTRIBUTING.md). No project license has been selected yet; this public fork does not declare an open-source license.
