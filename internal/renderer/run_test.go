package renderer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Feruum/Leafrun/internal/bundle"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	documentcore "github.com/leafrun/leafrun-document-core"
)

func fixtureBundle(t *testing.T, source string) renderwire.Bundle {
	t.Helper()
	files := map[string][]byte{
		"assets/logo.svg":   []byte(`<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20"><rect width="20" height="20" fill="green"/></svg>`),
		"input.json":        []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["n","title"],"properties":{"n":{"type":"number"},"title":{"type":"string"}}}`),
		"main.typ":          []byte(source),
		"sections/body.typ": []byte("A source section included by the entrypoint.\n"),
	}
	manifest := storagev1.SnapshotManifest{
		FormatVersion: 1,
		Origin:        storagev1.SourceOrigin{Kind: "folder", Location: "source-removed-after-capture"},
		Files:         []storagev1.SnapshotFile{},
		Directories:   []string{"assets", "empty", "empty/nested", "sections"},
		Fonts:         []storagev1.SnapshotFont{{Path: ".", SourcePath: "/removed/font/source", InProject: false, Files: []storagev1.SnapshotFile{}, Directories: []string{}}},
	}
	project := fstest.MapFS{}
	for name, data := range files {
		project[name] = &fstest.MapFile{Data: data}
		manifest.Files = append(manifest.Files, fileEntry(name, data))
	}
	sort.Slice(manifest.Files, func(i, j int) bool { return manifest.Files[i].Path < manifest.Files[j].Path })
	fontData := []byte("not a native font; the provenance path is intentionally absent")
	font := fstest.MapFS{"font-notes.txt": &fstest.MapFile{Data: fontData}}
	manifest.Fonts[0].Files = []storagev1.SnapshotFile{fileEntry("font-notes.txt", fontData)}
	id, err := storagev1.ManifestContentID(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = id
	return renderwire.Bundle{Manifest: manifest, ProjectFS: project, ExternalFontFS: map[int]fs.FS{0: font}}
}

func fileEntry(name string, data []byte) storagev1.SnapshotFile {
	sum := sha256.Sum256(data)
	return storagev1.SnapshotFile{Path: name, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
}

func TestRunPreparedSelectionStreamsNestedFilesAndFrozenProfile(t *testing.T) {
	if _, err := exec.LookPath("typst"); err != nil {
		t.Skip("Typst 0.15.1 is not on PATH")
	}
	ctx := context.Background()
	asset := []byte(`<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20"><rect width="20" height="20" fill="navy"/></svg>`)
	assetHash := sha256.Sum256(asset)
	assetSHA := hex.EncodeToString(assetHash[:])
	manifest := []byte(`{"formatVersion":1,"name":"Nested selection","engine":{"typstVersion":"0.15.1"},"templates":[{"id":"invoice","entrypoint":"templates/main.typ","inputSchema":"schema.json"}],"targets":[{"id":"invoice","kind":"document","template":"invoice","output":{"format":"pdf","path":"invoice.pdf"}}],"profiles":{"production":{"inputs":{"theme":"blue"},"pdfStandard":"a-2b"}},"defaultProfile":"production"}`)
	main := []byte(`#let data = json(sys.inputs.at("leafrun-data"))
#assert(sys.inputs.at("theme") == "blue")
#image(data.logo, width: 1cm)
= #sys.inputs.at("theme")
`)
	schema := []byte(`{"type":"object","properties":{"logo":{"type":"string","x-leafrun-file":true}},"required":["logo"]}`)
	source := selectionFixtureBundle(t, map[string][]byte{"leafrun.json": manifest, "schema.json": schema, "templates/main.typ": main})
	base, err := documentcore.RestoreSnapshot(ctx, toCoreBundle(source), documentcore.RestoreSnapshotOptions{TempDir: t.TempDir(), Limits: snapshotLimits})
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	project, err := documentcore.ReadProject(ctx, base.FS())
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := documentcore.DescribeTarget(ctx, base.FS(), project, base.Manifest(), "invoice", "production")
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := documentcore.PrepareParameters(ctx, base.FS(), descriptor.InputSchema, []byte(`{}`), []documentcore.FileRef{{Pointer: "/logo", ResourceID: "resource_logo", Filename: "logo.svg", SHA256: assetSHA, SizeBytes: int64(len(asset))}})
	if err != nil {
		t.Fatal(err)
	}
	prepared.Inputs["theme"] = "blue"
	preparedBytes, err := json.Marshal(prepared)
	if err != nil {
		t.Fatal(err)
	}
	var frozen projectsv1.PreparedInput
	if err = jsonvalue.Unmarshal(preparedBytes, &frozen, projectsv1.ResponseLimit); err != nil {
		t.Fatal(err)
	}
	request := renderwire.RenderRequest{
		Scope: storagev1.Scope{WorkspaceID: "workspace_1", ProjectID: "project_1"}, JobID: "job_selection", AttemptID: "attempt_selection",
		TargetID: descriptor.TargetID, Entrypoint: descriptor.Entrypoint, SourceSnapshotID: "snapshot_selection", SourceContentID: source.Manifest.ID,
		InputFingerprint: strings.Repeat("d", 64), Parameters: frozen.JSON, ParametersSchemaPath: descriptor.InputSchema,
		Execution: &renderwire.ExecutionInput{DescriptorDigest: descriptor.Digest, ExecutionDigest: descriptor.ExecutionDigest, Profile: descriptor.SelectedProfile, Prepared: frozen},
	}
	var input bytes.Buffer
	encoder := renderwire.NewEncoder(&input, renderwire.InputLimit)
	requestBytes, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err = encoder.WriteBytes(renderwire.Request, requestBytes); err != nil {
		t.Fatal(err)
	}
	if err = renderwire.WriteBundle(encoder, source); err != nil {
		t.Fatal(err)
	}
	if err = renderwire.WriteInputFiles(encoder, frozen.Files, func(file projectsv1.PreparedFile) (io.ReadCloser, error) {
		if file.ResourceID != "resource_logo" {
			return nil, fmt.Errorf("unexpected resource %q", file.ResourceID)
		}
		return io.NopCloser(bytes.NewReader(asset)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = encoder.WriteBytes(renderwire.Terminal, []byte(`{"complete":true}`)); err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	var output bytes.Buffer
	if err = Run(ctx, &input, &output, Options{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	decoded := decodeOutput(t, output.Bytes(), workDir)
	if decoded.derived != nil {
		defer decoded.derived.Close()
	}
	if !decoded.terminal.Success || decoded.pdf == nil || decoded.derived == nil {
		t.Fatalf("prepared selection did not render successfully: %+v", decoded.terminal)
	}
	if decoded.terminal.InputFingerprint != request.InputFingerprint || decoded.terminal.SourceSnapshotID != request.SourceSnapshotID {
		t.Fatalf("terminal lost frozen request identity: %+v", decoded.terminal)
	}
	derivedFS := decoded.derived.ProjectFS()
	storedJSON, err := fs.ReadFile(derivedFS, "input.json")
	if err != nil || !bytes.Equal(storedJSON, frozen.JSON) {
		t.Fatalf("prepared JSON changed in derived snapshot: %s, %v", storedJSON, err)
	}
	storedAsset, err := fs.ReadFile(derivedFS, frozen.Files[0].Path)
	if err != nil || !bytes.Equal(storedAsset, asset) {
		t.Fatalf("verified file bytes missing at %q: %v", frozen.Files[0].Path, err)
	}
}

func selectionFixtureBundle(t *testing.T, contents map[string][]byte) renderwire.Bundle {
	t.Helper()
	manifest := storagev1.SnapshotManifest{FormatVersion: 1, Origin: storagev1.SourceOrigin{Kind: "folder", Location: "captured"}, Files: []storagev1.SnapshotFile{}, Directories: []string{}, Fonts: []storagev1.SnapshotFont{}}
	project := fstest.MapFS{}
	directories := map[string]bool{}
	for name, data := range contents {
		project[name] = &fstest.MapFile{Data: bytes.Clone(data)}
		manifest.Files = append(manifest.Files, fileEntry(name, data))
		parent := path.Dir(name)
		for parent != "." {
			directories[parent] = true
			parent = path.Dir(parent)
		}
	}
	for directory := range directories {
		manifest.Directories = append(manifest.Directories, directory)
	}
	sort.Strings(manifest.Directories)
	sort.Slice(manifest.Files, func(i, j int) bool { return manifest.Files[i].Path < manifest.Files[j].Path })
	id, err := storagev1.ManifestContentID(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = id
	return renderwire.Bundle{Manifest: manifest, ProjectFS: project}
}

func requestFor(t *testing.T, manifest storagev1.SnapshotManifest, parameters []byte, schema string) renderwire.RenderRequest {
	t.Helper()
	request := renderwire.RenderRequest{
		Scope: storagev1.Scope{WorkspaceID: "workspace_1", ProjectID: "project_1"},
		JobID: "job_1", AttemptID: "attempt_1", TargetID: "typst",
		Entrypoint: "main.typ", SourceSnapshotID: "snapshot_1", SourceContentID: manifest.ID,
		Parameters: append(json.RawMessage(nil), parameters...), ParametersSchemaPath: schema,
	}
	job := generationv1.CreateJobRequest{Scope: request.Scope, SnapshotID: request.SourceSnapshotID, TargetID: request.TargetID, Entrypoint: request.Entrypoint, Parameters: request.Parameters, ParametersSchemaPath: request.ParametersSchemaPath}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	_, _, request.InputFingerprint, err = generationv1.NormalizeRequest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func inputStream(t *testing.T, request renderwire.RenderRequest, snapshot renderwire.Bundle) *bytes.Buffer {
	t.Helper()
	var input bytes.Buffer
	encoder := renderwire.NewEncoder(&input, renderwire.InputLimit)
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteBytes(renderwire.Request, data); err != nil {
		t.Fatal(err)
	}
	if err := renderwire.WriteBundle(encoder, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteBytes(renderwire.Terminal, []byte(`{"complete":true}`)); err != nil {
		t.Fatal(err)
	}
	return &input
}

type decodedOutput struct {
	terminal renderwire.TerminalEnvelope
	derived  *bundle.StagedBundle
	pdf      *renderwire.PDFDescriptor
	pdfHash  string
	pdfSize  int64
}

func decodeOutput(t *testing.T, output []byte, tempDir string) decodedOutput {
	t.Helper()
	decoder := renderwire.NewDecoder(bytes.NewReader(output), renderwire.OutputLimit)
	result := decodedOutput{}
	kind, err := decoder.PeekKind()
	if err != nil {
		t.Fatal(err)
	}
	if kind == renderwire.Manifest {
		result.derived, err = bundle.Stage(context.Background(), decoder, bundle.Options{TempDir: tempDir})
		if err != nil {
			t.Fatal("derived bundle:", err)
		}
		kind, err = decoder.PeekKind()
		if err != nil {
			t.Fatal(err)
		}
	}
	if kind == renderwire.PDFHeader {
		data, err := decoder.ReadBytes(renderwire.PDFHeader)
		if err != nil {
			t.Fatal(err)
		}
		var descriptor renderwire.PDFDescriptor
		if err := renderwire.DecodeJSON(data, &descriptor); err != nil {
			t.Fatal(err)
		}
		result.pdf = &descriptor
		hash := sha256.New()
		err = decoder.Read(renderwire.PDF, func(header renderwire.Header, reader io.Reader) error {
			result.pdfSize = header.Size
			_, copyErr := io.Copy(hash, reader)
			return copyErr
		})
		if err != nil {
			t.Fatal(err)
		}
		result.pdfHash = hex.EncodeToString(hash.Sum(nil))
	}
	terminalBytes, err := decoder.ReadBytes(renderwire.Terminal)
	if err != nil {
		t.Fatal(err)
	}
	if err := renderwire.DecodeJSON(terminalBytes, &result.terminal); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Finish(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRunDerivesExactParametersAndStreamsVerifiedPDF(t *testing.T) {
	if _, err := exec.LookPath("typst"); err != nil {
		t.Skip("Typst 0.15.1 is not on PATH")
	}
	main := `#let parameters = json("input.json")
#assert(type(parameters.n) == int)
= #parameters.title
#include "sections/body.typ"
#image("assets/logo.svg", width: 1cm)
`
	snapshot := fixtureBundle(t, main)
	params := []byte(`{"n":1,"title":"Integer 1"}`)
	request := requestFor(t, snapshot.Manifest, params, "input.json")
	workDir := t.TempDir()
	input := inputStream(t, request, snapshot)
	var output bytes.Buffer
	if err := Run(context.Background(), input, &output, Options{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	decoded := decodeOutput(t, output.Bytes(), workDir)
	if decoded.derived != nil {
		defer decoded.derived.Close()
	}
	if !decoded.terminal.Complete || !decoded.terminal.Success || decoded.terminal.Phase != "complete" {
		t.Fatalf("unexpected terminal: %+v", decoded.terminal)
	}
	if decoded.terminal.SourceSnapshotID != request.SourceSnapshotID || decoded.terminal.SourceContentID != request.SourceContentID || decoded.terminal.InputFingerprint != request.InputFingerprint {
		t.Fatalf("terminal binding changed: %+v", decoded.terminal)
	}
	if decoded.terminal.EngineVersion != "0.15.1" || decoded.pdf == nil || decoded.derived == nil {
		t.Fatalf("success omitted engine, PDF, or derived bundle: %+v", decoded.terminal)
	}
	if decoded.terminal.DerivedContentID != decoded.derived.Manifest().ID || decoded.derived.Manifest().ID == snapshot.Manifest.ID {
		t.Fatalf("derived content identity is wrong: terminal=%s bundle=%s", decoded.terminal.DerivedContentID, decoded.derived.Manifest().ID)
	}
	if decoded.pdf.SHA256 != decoded.pdfHash || decoded.pdf.Size != decoded.pdfSize || decoded.pdf.Size <= 5 {
		t.Fatalf("PDF descriptor did not match streamed bytes: descriptor=%+v hash=%s size=%d", decoded.pdf, decoded.pdfHash, decoded.pdfSize)
	}
	project := decoded.derived.ProjectFS()
	inputFile, err := fs.ReadFile(project, "input.json")
	if err != nil || !bytes.Equal(inputFile, params) {
		t.Fatalf("derived input.json changed exact normalized parameters: %q, %v", inputFile, err)
	}
	if _, err := fs.Stat(project, "empty/nested"); err != nil {
		t.Fatal("empty source directories were lost:", err)
	}
	if decoded.derived.Manifest().Fonts[0].SourcePath != "/removed/font/source" {
		t.Fatal("font provenance was not preserved")
	}
	if raw, err := fs.ReadFile(snapshot.ProjectFS, "input.json"); err != nil || !bytes.Contains(raw, []byte("draft/2020-12")) {
		t.Fatal("source schema was changed while deriving", err)
	}
}

func TestRunWithoutParametersRendersBaseWithoutDerivedBundle(t *testing.T) {
	if _, err := exec.LookPath("typst"); err != nil {
		t.Skip("Typst 0.15.1 is not on PATH")
	}
	snapshot := fixtureBundle(t, `= base snapshot render`)
	request := requestFor(t, snapshot.Manifest, nil, "")
	workDir := t.TempDir()
	var output bytes.Buffer
	if err := Run(context.Background(), inputStream(t, request, snapshot), &output, Options{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	decoded := decodeOutput(t, output.Bytes(), workDir)
	if !decoded.terminal.Success || decoded.terminal.Phase != "complete" || decoded.pdf == nil {
		t.Fatalf("unexpected base render terminal: %+v", decoded.terminal)
	}
	if decoded.derived != nil || decoded.terminal.DerivedContentID != "" {
		t.Fatalf("render without parameters returned a derived snapshot: %+v", decoded.terminal)
	}
	if len(decoded.terminal.SourceSnapshotID) == 0 || decoded.pdfHash != decoded.pdf.SHA256 {
		t.Fatalf("base render lost its binding or PDF digest: %+v", decoded.terminal)
	}
}

func TestRunKeepsJSONIntegerAndDecimalLexemesDistinctForTypst(t *testing.T) {
	if _, err := exec.LookPath("typst"); err != nil {
		t.Skip("Typst 0.15.1 is not on PATH")
	}
	main := `#let parameters = json("input.json")
#assert(type(parameters.n) == int)
= #parameters.n
`
	snapshot := fixtureBundle(t, main)
	var ids []string
	for _, params := range [][]byte{[]byte(`{"n":1,"title":"same"}`), []byte(`{"n":1.0,"title":"same"}`)} {
		request := requestFor(t, snapshot.Manifest, params, "input.json")
		workDir := t.TempDir()
		var output bytes.Buffer
		if err := Run(context.Background(), inputStream(t, request, snapshot), &output, Options{WorkDir: workDir}); err != nil {
			t.Fatal(err)
		}
		decoded := decodeOutput(t, output.Bytes(), workDir)
		if decoded.derived != nil {
			defer decoded.derived.Close()
		}
		if decoded.derived == nil {
			t.Fatalf("render did not return its derived bundle: %+v", decoded.terminal)
		}
		ids = append(ids, decoded.derived.Manifest().ID)
		stored, err := fs.ReadFile(decoded.derived.ProjectFS(), "input.json")
		if err != nil || !bytes.Equal(stored, params) {
			t.Fatalf("parameter bytes changed from %s to %s: %v", params, stored, err)
		}
		integer := !bytes.Contains(params, []byte("1.0"))
		if integer && !decoded.terminal.Success {
			t.Fatalf("integer JSON should satisfy Typst int assertion: %+v", decoded.terminal)
		}
		if !integer && decoded.terminal.Success {
			t.Fatalf("decimal JSON should remain a float for Typst: %+v", decoded.terminal)
		}
		if decoded.terminal.Success != integer {
			t.Fatalf("unexpected Typst number semantics for %s: %+v", params, decoded.terminal)
		}
	}
	if ids[0] == ids[1] {
		t.Fatalf("integer and decimal inputs shared derived identity %s", ids[0])
	}
}

func TestRunValidationFailureIsTerminalOnlyAndDoesNotInvokeTypst(t *testing.T) {
	snapshot := fixtureBundle(t, `= Should never reach Typst`)
	params := []byte(`{"n":"not a number","title":"bad"}`)
	request := requestFor(t, snapshot.Manifest, params, "input.json")
	t.Setenv("PATH", t.TempDir())
	workDir := t.TempDir()
	var output bytes.Buffer
	if err := Run(context.Background(), inputStream(t, request, snapshot), &output, Options{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	decoded := decodeOutput(t, output.Bytes(), workDir)
	if decoded.derived != nil {
		defer decoded.derived.Close()
	}
	if decoded.terminal.Success || decoded.terminal.Phase != "validation" || decoded.terminal.FailureCode != "parameters_invalid" {
		t.Fatalf("unexpected validation terminal: %+v", decoded.terminal)
	}
	if decoded.terminal.EngineVersion != "0.15.1" {
		t.Fatalf("validation terminal lacks pinned engine binding: %q", decoded.terminal.EngineVersion)
	}
	if decoded.derived != nil || decoded.pdf != nil {
		t.Fatal("validation failure emitted a bundle or PDF")
	}
}

func TestRunCompileFailureCanRetainDerivedBundleButNeverPDF(t *testing.T) {
	if _, err := exec.LookPath("typst"); err != nil {
		t.Skip("Typst 0.15.1 is not on PATH")
	}
	snapshot := fixtureBundle(t, `#let broken = (`)
	params := []byte(`{"n":1,"title":"valid"}`)
	request := requestFor(t, snapshot.Manifest, params, "input.json")
	workDir := t.TempDir()
	var output bytes.Buffer
	if err := Run(context.Background(), inputStream(t, request, snapshot), &output, Options{WorkDir: workDir}); err != nil {
		t.Fatal("handled compile failure must exit cleanly:", err)
	}
	decoded := decodeOutput(t, output.Bytes(), workDir)
	if decoded.derived != nil {
		defer decoded.derived.Close()
	}
	if decoded.terminal.Success || decoded.terminal.Phase != "compile" || decoded.terminal.FailureCode != "compile_failed" {
		t.Fatalf("unexpected compile terminal: %+v", decoded.terminal)
	}
	if decoded.derived == nil || decoded.pdf != nil {
		t.Fatal("compile failure must retain derived history and omit PDF")
	}
	if decoded.terminal.DerivedContentID != decoded.derived.Manifest().ID {
		t.Fatalf("failure did not bind the derived ID: %+v", decoded.terminal)
	}
}

func TestRunWireFailureEmitsNoTerminalAndCleansWorkRoot(t *testing.T) {
	snapshot := fixtureBundle(t, `= no rendering`)
	request := requestFor(t, snapshot.Manifest, nil, "")
	input := inputStream(t, request, snapshot).Bytes()
	input[len(input)-1] ^= 0xff
	workDir := t.TempDir()
	var output bytes.Buffer
	if err := Run(context.Background(), bytes.NewReader(input), &output, Options{WorkDir: workDir}); err == nil {
		t.Fatal("accepted a corrupt input terminal")
	}
	if output.Len() != 0 {
		t.Fatal("wire failure emitted a publishable-looking terminal")
	}
	entries, err := os.ReadDir(workDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("renderer leaked tmpfs state: %v %v", entries, err)
	}
}

func TestRunOutputFailureIsInfrastructureError(t *testing.T) {
	if _, err := exec.LookPath("typst"); err != nil {
		t.Skip("Typst 0.15.1 is not on PATH")
	}
	snapshot := fixtureBundle(t, `= output failure`)
	request := requestFor(t, snapshot.Manifest, nil, "")
	writer := failingWriter{}
	if err := Run(context.Background(), inputStream(t, request, snapshot), writer, Options{WorkDir: t.TempDir()}); err == nil {
		t.Fatal("ignored output stream failure")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestCreateInputSnapshotDirectoryCollisionIsHandledValidation(t *testing.T) {
	snapshot := fixtureBundle(t, `= Should not reach Typst`)
	delete(snapshot.ProjectFS.(fstest.MapFS), "input.json")
	data := []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"n":{"type":"number"}},"required":["n"]}`)
	project := snapshot.ProjectFS.(fstest.MapFS)
	project["schema.json"] = &fstest.MapFile{Data: data}
	project["input.json/child.txt"] = &fstest.MapFile{Data: []byte("not overlayable")}
	snapshot.Manifest.Files = nil
	for name, file := range project {
		snapshot.Manifest.Files = append(snapshot.Manifest.Files, fileEntry(name, file.Data))
	}
	sort.Slice(snapshot.Manifest.Files, func(i, j int) bool { return snapshot.Manifest.Files[i].Path < snapshot.Manifest.Files[j].Path })
	snapshot.Manifest.Directories = append(snapshot.Manifest.Directories, "input.json")
	sort.Strings(snapshot.Manifest.Directories)
	id, err := storagev1.ManifestContentID(snapshot.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Manifest.ID = id
	request := requestFor(t, snapshot.Manifest, []byte(`{"n":1}`), "schema.json")
	t.Setenv("PATH", t.TempDir())
	workDir := t.TempDir()
	var output bytes.Buffer
	if err := Run(context.Background(), inputStream(t, request, snapshot), &output, Options{WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	decoded := decodeOutput(t, output.Bytes(), workDir)
	if decoded.terminal.Success || decoded.terminal.Phase != "validation" || decoded.derived != nil || decoded.pdf != nil {
		t.Fatalf("input directory collision was not a pre-render validation failure: %+v", decoded.terminal)
	}
}
