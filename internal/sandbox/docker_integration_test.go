package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"testing"
	"testing/fstest"
	"time"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func liveDocker(t *testing.T) (*Docker, context.Context, Spec) {
	t.Helper()
	if os.Getenv("LEAFRUN_E04_SANDBOX_INTEGRATION") != "1" {
		t.Skip("real renderer sandbox fixture is disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	d, err := NewDocker()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err = d.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	return d, ctx, Spec{JobID: "sandbox-integration", AttemptID: t.Name(), CreateKey: time.Now().UTC().Format("150405.000000000"), Image: os.Getenv("LEAFRUN_RENDERER_IMAGE")}
}

func testCapturedBundle(t *testing.T, source string) renderwire.Bundle {
	t.Helper()
	files := map[string]string{"main.typ": source, "input.json": `{"type":"object","properties":{"integer":{"type":"integer"},"float":{"type":"number"},"exponent":{"type":"number"}}}`}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	m := storagev1.SnapshotManifest{FormatVersion: 1, Files: []storagev1.SnapshotFile{}, Directories: []string{"empty"}, Fonts: []storagev1.SnapshotFont{}}
	fs := fstest.MapFS{"empty": &fstest.MapFile{Mode: os.ModeDir | 0700}}
	for _, name := range names {
		data := []byte(files[name])
		hash := sha256.Sum256(data)
		m.Files = append(m.Files, storagev1.SnapshotFile{Path: name, Size: int64(len(data)), SHA256: hex.EncodeToString(hash[:])})
		fs[name] = &fstest.MapFile{Data: data}
	}
	var err error
	m.ID, err = storagev1.ManifestContentID(m)
	if err != nil {
		t.Fatal(err)
	}
	return renderwire.Bundle{Manifest: m, ProjectFS: fs}
}

func testRenderRequest(t *testing.T, s Spec, source renderwire.Bundle) renderwire.RenderRequest {
	t.Helper()
	raw, _ := json.Marshal(generationv1.CreateJobRequest{Scope: storagev1.Scope{WorkspaceID: "demo", ProjectID: "roundtrip"}, SnapshotID: "storage-snapshot", TargetID: "pdf", Entrypoint: "main.typ", Parameters: json.RawMessage(`{"integer":1,"float":1.0,"exponent":1e0}`), ParametersSchemaPath: "input.json"})
	r, _, fingerprint, err := generationv1.NormalizeRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	return renderwire.RenderRequest{Scope: r.Scope, JobID: s.JobID, AttemptID: s.AttemptID, TargetID: r.TargetID, Entrypoint: r.Entrypoint, SourceSnapshotID: r.SnapshotID, SourceContentID: source.Manifest.ID, InputFingerprint: fingerprint, Parameters: r.Parameters, ParametersSchemaPath: r.ParametersSchemaPath}
}

func TestLiveDockerTransfersResultBeforeExitAndPreservesNumericTypes(t *testing.T) {
	d, ctx, spec := liveDocker(t)
	source := testCapturedBundle(t, `#let p = json("input.json")
#assert(type(p.integer) == int)
#assert(type(p.float) == float)
#assert(type(p.exponent) == float)
Numeric types survived the snapshot transport.`)
	request := testRenderRequest(t, spec, source)
	id, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = d.Kill(cleanup, id)
		_ = d.Remove(cleanup, id)
	})
	if _, err = d.Inspect(ctx, id, spec); err != nil {
		t.Fatal(err)
	}
	attached, err := d.Attach(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer attached.Close()
	if err = d.Start(ctx, id); err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() {
		writeErr := WriteInput(attached.Conn, request, source)
		written <- errors.Join(writeErr, attached.CloseWrite())
	}()
	result, err := ReadOutput(ctx, NewDockerStdout(attached.Reader), t.TempDir(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if err = <-written; err != nil {
		t.Fatal(err)
	}
	state, err := d.Inspect(ctx, id, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Successful() || !result.Terminal.Success || result.Derived == nil {
		t.Fatalf("container/result unsuccessful: %+v %+v", state, result.Terminal)
	}
	if err = result.Confirm(spec, state); err != nil {
		t.Fatal(err)
	}
	pdf, err := result.OpenPDF()
	if err != nil {
		t.Fatal(err)
	}
	defer pdf.Close()
	magic := make([]byte, 5)
	if _, err = io.ReadFull(pdf, magic); err != nil || string(magic) != "%PDF-" {
		t.Fatalf("PDF did not leave tmpfs: %q %v", magic, err)
	}
}
