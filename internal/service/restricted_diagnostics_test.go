package service

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestRestrictedJobHasNoSourceSensitiveFields(t *testing.T) {
	server := &Server{cfg: Config{Access: &testAccess{allowed: true, denied: map[string]bool{"source.read": true}}}}
	job := generationv1.Job{JobID: "job", Scope: storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}, Status: generationv1.Failed, Entrypoint: "secret-path.typ", RawDiagnostics: "secret-raw", Parameters: json.RawMessage(`{"secret-parameter":1}`), Phase: "secret-phase", FailureCode: "secret-code"}
	if err := json.Unmarshal([]byte(`[{"severity":"error","message":"secret-message","hints":["secret-hint"],"trace":["secret-trace"]}]`), &job.Diagnostics); err != nil {
		t.Fatal(err)
	}
	if err := server.redactSourceFields(context.Background(), storagev1.Principal{Subject: "alice", Domain: "user"}, &job); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(job)
	if strings.Contains(string(data), "secret-") {
		t.Fatalf("source-sensitive data exposed: %s", data)
	}
	var result map[string]any
	_ = json.Unmarshal(data, &result)
	if result["diagnosticsVisibility"] != "restricted" {
		t.Fatalf("missing restricted_reader contract: %s", data)
	}
}

func TestRestrictedProjectsErrorHidesAllProviderFields(t *testing.T) {
	server := &Server{}
	err := &projectsv1.HTTPError{Status: 422, Code: "invalid_selection", Message: "secret-summary", Fields: []projectsv1.FieldError{{File: "secret-file", Pointer: "/secret-pointer", Code: "secret-code", Message: "secret-message"}}}
	w := httptest.NewRecorder()
	server.writeProjectsError(w, err)
	if w.Code != 422 || strings.Contains(w.Body.String(), "secret-") || strings.Contains(w.Body.String(), "fields") {
		t.Fatalf("unsafe error: %d %s", w.Code, w.Body)
	}
}
