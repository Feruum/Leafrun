package sandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func outputFixture(t *testing.T) ([]byte, renderwire.RenderRequest, Spec) {
	t.Helper()
	request := renderwire.RenderRequest{Scope: storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}, JobID: "job", AttemptID: "attempt", TargetID: "target", SourceSnapshotID: "snapshot", SourceContentID: "content", InputFingerprint: "fingerprint"}
	pdf := []byte("%PDF-fixture\x00\xff")
	hash := sha256.Sum256(pdf)
	descriptor := renderwire.PDFDescriptor{Size: int64(len(pdf)), SHA256: hex.EncodeToString(hash[:])}
	terminal := renderwire.TerminalEnvelope{Complete: true, Success: true, Scope: request.Scope, JobID: request.JobID, AttemptID: request.AttemptID, TargetID: request.TargetID, InputFingerprint: request.InputFingerprint, SourceSnapshotID: request.SourceSnapshotID, SourceContentID: request.SourceContentID, EngineVersion: "0.15.1", Phase: "complete", PDF: &descriptor}
	var wire bytes.Buffer
	e := renderwire.NewEncoder(&wire, renderwire.OutputLimit)
	for _, frame := range []struct {
		kind  renderwire.Kind
		value any
	}{{renderwire.PDFHeader, descriptor}, {renderwire.PDF, pdf}, {renderwire.Terminal, terminal}} {
		var data []byte
		if frame.kind == renderwire.PDF {
			data = pdf
		} else {
			data, _ = json.Marshal(frame.value)
		}
		if err := e.WriteBytes(frame.kind, data); err != nil {
			t.Fatal(err)
		}
	}
	return wire.Bytes(), request, Spec{JobID: request.JobID, AttemptID: request.AttemptID, CreateKey: "create", Image: "sha256:" + strings.Repeat("a", 64)}
}

func TestOutputNeedsCompleteWireAndConfirmedExitBeforeRecovery(t *testing.T) {
	wire, request, spec := outputFixture(t)
	parent := t.TempDir()
	result, err := ReadOutput(context.Background(), bytes.NewReader(wire), parent, request)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if _, err = ReopenOutput(context.Background(), parent, result.Token(), request, spec, strings.Repeat("b", 64)); err == nil {
		t.Fatal("unconfirmed result recovered")
	}
	if err = result.Confirm(spec, State{ID: strings.Repeat("b", 64), Status: "exited", ExitCode: 1}); err == nil {
		t.Fatal("nonzero exit accepted")
	}
	if err = result.Confirm(spec, State{ID: strings.Repeat("b", 64), Status: "exited", ExitCode: 0}); err != nil {
		t.Fatal(err)
	}
	reopened, err := ReopenOutput(context.Background(), parent, result.Token(), request, spec, strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	file, err := reopened.OpenPDF()
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	if err = os.WriteFile(result.pdfPath, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = ReopenOutput(context.Background(), parent, result.Token(), request, spec, strings.Repeat("b", 64)); err == nil {
		t.Fatal("mutated PDF recovered")
	}
}

func TestOutputRejectsPartialExtraAndWrongBinding(t *testing.T) {
	wire, request, _ := outputFixture(t)
	for _, data := range [][]byte{wire[:len(wire)-1], append(append([]byte(nil), wire...), 0)} {
		if result, err := ReadOutput(context.Background(), bytes.NewReader(data), t.TempDir(), request); err == nil {
			result.Close()
			t.Fatal("partial/trailing stream accepted")
		}
	}
	request.InputFingerprint = "different"
	if result, err := ReadOutput(context.Background(), bytes.NewReader(wire), t.TempDir(), request); err == nil {
		result.Close()
		t.Fatal("wrong result binding accepted")
	}
}
