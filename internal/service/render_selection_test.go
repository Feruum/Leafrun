package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestRenderSelectionRejectsServerOwnedFileMetadata(t *testing.T) {
	server := newTestServer(t, newTestStore(), &testAccess{allowed: true}, &testPins{})
	body := `{"scope":{"workspaceId":"ws-1","projectId":"project-1"},"releaseId":"release-1","fileRefs":{"/logo":{"resourceId":"resource-1","filename":"logo.svg","sizeBytes":12,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","path":".leafrun-input/logo.svg"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/renders", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "selection-key")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, req)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("server-owned file metadata status = %d, want 400; body %s", response.Code, response.Body)
	}
}

func TestPublicRoutesRejectCallerSuppliedResourceMetadata(t *testing.T) {
	legacy := newTestStore()
	legacyServer := newTestServer(t, legacy, &testAccess{allowed: true}, &testPins{})
	legacyBody := strings.TrimSuffix(createBody, "}") + `,"fileRefs":{"/logo":{"resourceId":"resource-1","filename":"logo.svg","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sizeBytes":12}}}`
	legacyResponse := createRequest(legacyServer, legacyBody, "legacy-server-fields")
	if legacyResponse.Code != http.StatusBadRequest || legacy.admitCalls != 0 {
		t.Fatalf("legacy route accepted Projects-owned fields: status=%d admissions=%d body=%s", legacyResponse.Code, legacy.admitCalls, legacyResponse.Body)
	}

	selection := `{"scope":{"workspaceId":"ws-1","projectId":"project-1"},"releaseId":"release-1","fileRefs":{"/logo":{"resourceId":"resource-1","filename":"logo.svg","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sizeBytes":12}}}`
	selectionServer := newTestServer(t, newTestStore(), &testAccess{allowed: true}, &testPins{})
	selectionResponse := postRenderSelection(selectionServer, selection, "selection-server-fields")
	if selectionResponse.Code != http.StatusBadRequest {
		t.Fatalf("selection route accepted Projects-owned fields: status=%d body=%s", selectionResponse.Code, selectionResponse.Body)
	}
}

type selectionProjects struct {
	execution     projectsv1.ResolvedExecution
	resolveErr    error
	resolveCall   int
	current       projectsv1.CurrentReleaseDecision
	currentErr    error
	currentChecks []projectsv1.CurrentReleaseCheck
}

func (p *selectionProjects) Resolve(_ context.Context, request projectsv1.ResolveRequest) (projectsv1.ResolvedExecution, error) {
	p.resolveCall++
	if p.resolveErr != nil {
		return projectsv1.ResolvedExecution{}, p.resolveErr
	}
	return p.execution, nil
}

func (p *selectionProjects) CurrentRelease(_ context.Context, check projectsv1.CurrentReleaseCheck) (projectsv1.CurrentReleaseDecision, error) {
	p.currentChecks = append(p.currentChecks, check)
	return p.current, p.currentErr
}

type selectionResources map[string]storagev1.Resource

func (resources selectionResources) GetResource(_ context.Context, id string) (storagev1.Resource, error) {
	resource, exists := resources[id]
	if !exists {
		return storagev1.Resource{}, &HTTPStatusError{StatusCode: http.StatusNotFound}
	}
	return resource, nil
}

func TestRenderSelectionReplayUsesOriginalReceiptAndResourcePinsPrecedeWorkflow(t *testing.T) {
	state := openHTTPStoreTest(t)
	access := &testAccess{allowed: true, denied: map[string]bool{"source.read": true}}
	pins := &testPins{}
	configureTestSnapshot(t, pins)
	selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, ReleaseID: "release-1", Parameters: json.RawMessage(`{}`), FileRefs: map[string]generationv1.FileReference{
		"/assetA": {ResourceID: "resource-file", Filename: "source.pdf"},
		"/assetB": {ResourceID: "resource-file", Filename: "source.txt"},
	}}
	_, selectionBytes, selectionFingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
	if err != nil {
		t.Fatal(err)
	}
	principal := storagev1.Principal{Subject: "user-1", Domain: "core"}
	resolved := validResolvedSelection(t, selection, selectionFingerprint, principal)
	projects := &selectionProjects{execution: resolved, current: projectsv1.CurrentReleaseDecision{Allowed: true}}
	resource := storagev1.Resource{ResourceID: "resource-file", Scope: selection.Scope, Class: storagev1.ClassResource, SHA256: resolved.Prepared.Files[0].SHA256, SizeBytes: resolved.Prepared.Files[0].SizeBytes, CreatedAt: time.Now()}
	server, err := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true,
		Credentials: StaticTokens{"valid-token": principal}, Access: access, Pins: pins, Resources: selectionResources{"resource-file": resource}, Projects: projects,
		OwnerToken: "owner-secret", EngineVersion: "0.15.1", Ready: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	pins.beforeActivate = func(storagev1.Pin) error {
		pending, pendingErr := state.PendingAdmissions(context.Background(), 10)
		if pendingErr != nil || len(pending) != 1 {
			return fmt.Errorf("pin activation did not precede workflow-start state: ids=%v err=%v", pending, pendingErr)
		}
		active, activeErr := state.AdmissionActivated(context.Background(), pending[0])
		if activeErr != nil || active {
			return fmt.Errorf("workflow start existed before all admission pins activated: active=%v err=%v", active, activeErr)
		}
		return nil
	}
	body := string(selectionBytes)
	first := postRenderSelection(server, body, "selection-replay")
	if first.Code != http.StatusAccepted {
		t.Fatalf("render selection status = %d, body %s", first.Code, first.Body)
	}
	if got := strings.Join(pins.activatedObjects, ","); got != "resource-file,snap-1" {
		t.Fatalf("pin activation order = %q", got)
	}
	if projects.resolveCall != 1 {
		t.Fatalf("initial resolve calls = %d, want 1", projects.resolveCall)
	}
	initiator, err := EncodeInitiator(principal)
	if err != nil {
		t.Fatal(err)
	}
	admission, found, err := state.LookupAdmission(context.Background(), selection.Scope.WorkspaceID, initiator, "selection-replay", selectionFingerprint)
	if err != nil || !found {
		t.Fatalf("original selection receipt = found %v, err %v", found, err)
	}
	input, err := state.GetExecutionInput(context.Background(), admission.Job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if input.Execution == nil || len(input.InputResources) != 2 || input.InputResources[0].PinID == "" || input.InputResources[0].PinID != input.InputResources[1].PinID {
		t.Fatalf("durable input resource pins = %+v", input.InputResources)
	}

	projects.resolveErr = errors.New("resolver must not run on an exact receipt replay")
	replay := postRenderSelection(server, body, "selection-replay")
	if replay.Code != http.StatusAccepted || projects.resolveCall != 1 {
		t.Fatalf("exact replay re-resolved selection: status=%d calls=%d body=%s", replay.Code, projects.resolveCall, replay.Body)
	}
	changed := strings.Replace(body, `"release-1"`, `"release-2"`, 1)
	conflict := postRenderSelection(server, changed, "selection-replay")
	if conflict.Code != http.StatusConflict || projects.resolveCall != 1 {
		t.Fatalf("changed selection key status=%d calls=%d body=%s", conflict.Code, projects.resolveCall, conflict.Body)
	}
	if deniedRead := access.denied["source.read"]; !deniedRead {
		t.Fatal("test did not deny source.read for the published release")
	}
	if err = state.FailJob(context.Background(), store.TerminalInput{JobID: admission.Job.JobID, FailureCode: "test_cleanup"}); err != nil {
		t.Fatal(err)
	}
	cleanup, err := state.PendingCleanup(context.Background(), admission.Job.JobID, 10)
	if err != nil {
		t.Fatal(err)
	}
	var cleanupObjects = map[string]bool{}
	for _, intent := range cleanup {
		if intent.Kind != "pin.release" {
			continue
		}
		var request struct {
			ObjectID string `json:"objectId"`
		}
		if err = json.Unmarshal(intent.Request, &request); err != nil {
			t.Fatal(err)
		}
		cleanupObjects[request.ObjectID] = true
	}
	if len(cleanupObjects) != 2 || !cleanupObjects["resource-file"] || !cleanupObjects["snap-1"] {
		t.Fatalf("terminal cleanup does not cover source and shared input pins: %v", cleanupObjects)
	}
}

func TestTerminalRenderSelectionReplayReturnsSavedAdmissionWithoutPinRecovery(t *testing.T) {
	state, pool := openHTTPStoreTestWithPool(t)
	access := &testAccess{allowed: true}
	pins := &testPins{loseFirstAck: true}
	configureTestSnapshot(t, pins)
	selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, ReleaseID: "release-1", Parameters: json.RawMessage(`{"title":"saved"}`)}
	_, normalized, fingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
	if err != nil {
		t.Fatal(err)
	}
	principal := storagev1.Principal{Subject: "user-1", Domain: "core"}
	projects := &selectionProjects{execution: validResolvedSelection(t, selection, fingerprint, principal), current: projectsv1.CurrentReleaseDecision{Allowed: true}}
	server, err := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true, Credentials: StaticTokens{"valid-token": principal}, Access: access, Pins: pins, Projects: projects, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
	if err != nil {
		t.Fatal(err)
	}
	first := postRenderSelection(server, string(normalized), "terminal-selection-replay")
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("lost acquire acknowledgement status=%d body=%s", first.Code, first.Body)
	}
	initiator, err := EncodeInitiator(principal)
	if err != nil {
		t.Fatal(err)
	}
	admission, found, err := state.LookupAdmission(context.Background(), selection.Scope.WorkspaceID, initiator, "terminal-selection-replay", fingerprint)
	if err != nil || !found {
		t.Fatalf("saved receipt after lost pin acknowledgement: found=%v err=%v", found, err)
	}
	if active, activeErr := state.AdmissionActivated(context.Background(), admission.Job.JobID); activeErr != nil || active {
		t.Fatalf("workflow start existed before expiration: active=%v err=%v", active, activeErr)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_jobs SET deadline=clock_timestamp()-interval '1 second' WHERE job_id=$1`, admission.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if expired, expireErr := state.ExpireDue(context.Background(), 10); expireErr != nil || expired != 1 {
		t.Fatalf("expire pending admission: count=%d err=%v", expired, expireErr)
	}
	admission, found, err = state.LookupAdmission(context.Background(), selection.Scope.WorkspaceID, initiator, "terminal-selection-replay", fingerprint)
	if err != nil || !found || admission.Job.Status != generationv1.Expired || !time.Now().Before(admission.IdempotencyExpiresAt) {
		t.Fatalf("live receipt did not preserve expired job: found=%v status=%s receiptExpiry=%v err=%v", found, admission.Job.Status, admission.IdempotencyExpiresAt, err)
	}
	if active, activeErr := state.AdmissionActivated(context.Background(), admission.Job.JobID); activeErr != nil || active {
		t.Fatalf("expired admission acquired a workflow start: active=%v err=%v", active, activeErr)
	}

	access.denied = map[string]bool{"source.read": true}
	replay := postRenderSelection(server, string(normalized), "terminal-selection-replay")
	var replayed generationv1.Admission
	if err = json.Unmarshal(replay.Body.Bytes(), &replayed); err != nil {
		t.Fatalf("decode terminal receipt replay: %v body=%s", err, replay.Body)
	}
	if replay.Code != http.StatusAccepted || replayed.Job.JobID != admission.Job.JobID || replayed.Job.Status != generationv1.Expired || len(replayed.Job.Parameters) != 0 {
		t.Fatalf("terminal replay did not return the same redacted job: status=%d job=%+v body=%s", replay.Code, replayed.Job, replay.Body)
	}
	if projects.resolveCall != 1 || len(pins.acquireKeys) != 1 || pins.activateCalls != 0 {
		t.Fatalf("terminal replay attempted recovery or re-resolution: resolves=%d acquisitions=%v activations=%d", projects.resolveCall, pins.acquireKeys, pins.activateCalls)
	}
	var workflows int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_workflow_starts WHERE job_id=$1`, admission.Job.JobID).Scan(&workflows); err != nil || workflows != 0 {
		t.Fatalf("terminal replay created workflow start: count=%d err=%v", workflows, err)
	}

	changed := selection
	changed.ReleaseID = "release-2"
	_, changedBytes, _, err := generationv1.NormalizeSelection(mustJSON(t, changed))
	if err != nil {
		t.Fatal(err)
	}
	conflict := postRenderSelection(server, string(changedBytes), "terminal-selection-replay")
	if conflict.Code != http.StatusConflict || projects.resolveCall != 1 || len(pins.acquireKeys) != 1 {
		t.Fatalf("changed terminal receipt selection was not rejected before recovery: status=%d resolves=%d acquisitions=%v body=%s", conflict.Code, projects.resolveCall, pins.acquireKeys, conflict.Body)
	}
}

func TestRenderSelectionRecoversLostInputPinAcquireAcknowledgementAfterRestart(t *testing.T) {
	state := openHTTPStoreTest(t)
	access := &testAccess{allowed: true}
	pins := &testPins{loseFirstAck: true}
	configureTestSnapshot(t, pins)
	selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, ReleaseID: "release-1", FileRefs: map[string]generationv1.FileReference{
		"/logo": {ResourceID: "resource-file", Filename: "logo.svg"},
	}}
	_, normalized, fingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
	if err != nil {
		t.Fatal(err)
	}
	principal := storagev1.Principal{Subject: "user-1", Domain: "core"}
	resolved := validResolvedSelection(t, selection, fingerprint, principal)
	file := resolved.Prepared.Files[0]
	projects := &selectionProjects{execution: resolved, current: projectsv1.CurrentReleaseDecision{Allowed: true}}
	resources := selectionResources{"resource-file": {ResourceID: "resource-file", Scope: selection.Scope, Class: storagev1.ClassResource, SHA256: file.SHA256, SizeBytes: file.SizeBytes, CreatedAt: time.Now()}}
	newServer := func() *Server {
		server, newErr := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true, Credentials: StaticTokens{"valid-token": principal}, Access: access, Pins: pins, Resources: resources, Projects: projects, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
		if newErr != nil {
			t.Fatal(newErr)
		}
		return server
	}
	first := postRenderSelection(newServer(), string(normalized), "lost-input-pin-ack")
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("lost input pin acknowledgement status=%d body=%s", first.Code, first.Body)
	}
	initiator, _ := EncodeInitiator(principal)
	admission, found, err := state.LookupAdmission(context.Background(), selection.Scope.WorkspaceID, initiator, "lost-input-pin-ack", fingerprint)
	if err != nil || !found {
		t.Fatalf("saved admission after lost pin acknowledgement: found=%v err=%v", found, err)
	}
	active, err := state.AdmissionActivated(context.Background(), admission.Job.JobID)
	if err != nil || active {
		t.Fatalf("workflow start preceded input pin activation: active=%v err=%v", active, err)
	}
	projects.resolveErr = errors.New("restart replay must not resolve current Projects state")
	second := postRenderSelection(newServer(), string(normalized), "lost-input-pin-ack")
	if second.Code != http.StatusAccepted || projects.resolveCall != 1 {
		t.Fatalf("restarted replay status=%d resolves=%d body=%s", second.Code, projects.resolveCall, second.Body)
	}
	if len(pins.acquireKeys) != 3 || pins.acquireKeys[0] == "" || pins.acquireKeys[0] != pins.acquireKeys[1] || pins.acquireKeys[2] == pins.acquireKeys[0] {
		t.Fatalf("lost input pin acknowledgement changed acquisition identity: %v", pins.acquireKeys)
	}
	if got := strings.Join(pins.activatedObjects, ","); got != "resource-file,snap-1" {
		t.Fatalf("input/source activation order after recovery = %q", got)
	}
}

func TestRenderSelectionRotatesInputPinKeyOnlyForTypedExpiry(t *testing.T) {
	state := openHTTPStoreTest(t)
	access := &testAccess{allowed: true}
	pins := &testPins{expireFirst: true}
	configureTestSnapshot(t, pins)
	selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, ReleaseID: "release-1", FileRefs: map[string]generationv1.FileReference{
		"/logo": {ResourceID: "resource-file", Filename: "logo.svg"},
	}}
	_, normalized, fingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
	if err != nil {
		t.Fatal(err)
	}
	principal := storagev1.Principal{Subject: "user-1", Domain: "core"}
	resolved := validResolvedSelection(t, selection, fingerprint, principal)
	file := resolved.Prepared.Files[0]
	projects := &selectionProjects{execution: resolved, current: projectsv1.CurrentReleaseDecision{Allowed: true}}
	resources := selectionResources{"resource-file": {ResourceID: "resource-file", Scope: selection.Scope, Class: storagev1.ClassResource, SHA256: file.SHA256, SizeBytes: file.SizeBytes, CreatedAt: time.Now()}}
	server, err := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true, Credentials: StaticTokens{"valid-token": principal}, Access: access, Pins: pins, Resources: resources, Projects: projects, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
	if err != nil {
		t.Fatal(err)
	}
	response := postRenderSelection(server, string(normalized), "expired-input-pin")
	if response.Code != http.StatusAccepted {
		t.Fatalf("expired input pin status=%d body=%s", response.Code, response.Body)
	}
	if len(pins.acquireKeys) != 3 || pins.acquireKeys[0] == pins.acquireKeys[1] || pins.acquireKeys[2] == pins.acquireKeys[1] {
		t.Fatalf("typed input pin expiry did not rotate before retry: %v", pins.acquireKeys)
	}
}

func TestExpiredInputResourceDoesNotRotatePinOrStartWorkflow(t *testing.T) {
	state := openHTTPStoreTest(t)
	pins := &testPins{acquireFirstError: &HTTPStatusError{StatusCode: http.StatusGone, Code: "expired"}}
	configureTestSnapshot(t, pins)
	selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, ReleaseID: "release-1", FileRefs: map[string]generationv1.FileReference{
		"/logo": {ResourceID: "resource-file", Filename: "logo.svg"},
	}}
	_, normalized, fingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
	if err != nil {
		t.Fatal(err)
	}
	principal := storagev1.Principal{Subject: "user-1", Domain: "core"}
	resolved := validResolvedSelection(t, selection, fingerprint, principal)
	file := resolved.Prepared.Files[0]
	resources := selectionResources{"resource-file": {ResourceID: "resource-file", Scope: selection.Scope, Class: storagev1.ClassResource, SHA256: file.SHA256, SizeBytes: file.SizeBytes}}
	server, err := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true, Credentials: StaticTokens{"valid-token": principal}, Access: &testAccess{allowed: true}, Pins: pins, Resources: resources, Projects: &selectionProjects{execution: resolved, current: projectsv1.CurrentReleaseDecision{Allowed: true}}, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
	if err != nil {
		t.Fatal(err)
	}
	response := postRenderSelection(server, string(normalized), "expired-resource-no-pin-rotation")
	if response.Code != 503 || len(pins.acquireKeys) != 1 {
		t.Fatalf("expired input resource changed acquisition identity: status=%d keys=%v", response.Code, pins.acquireKeys)
	}
	initiator, _ := EncodeInitiator(principal)
	admission, found, err := state.LookupAdmission(context.Background(), selection.Scope.WorkspaceID, initiator, "expired-resource-no-pin-rotation", fingerprint)
	if err != nil || !found {
		t.Fatalf("pending admission receipt: %v %v", found, err)
	}
	acquire, err := state.LatestPinAcquire(context.Background(), admission.Job.JobID, file.ResourceID)
	if err != nil || acquire.Number != 1 || acquire.State != "pending" || acquire.Response != nil || acquire.Key != pins.acquireKeys[0] {
		t.Fatalf("generic resource expiry changed the durable input acquisition: number=%d state=%s err=%v", acquire.Number, acquire.State, err)
	}
	active, err := state.AdmissionActivated(context.Background(), admission.Job.JobID)
	if err != nil || active {
		t.Fatalf("workflow started after input resource expiry: %v %v", active, err)
	}
}

func TestRenderWorkingSelectionRequiresSourceReadBeforeResolve(t *testing.T) {
	state := openHTTPStoreTest(t)
	access := &testAccess{allowed: true, denied: map[string]bool{"source.read": true}}
	pins := &testPins{}
	configureTestSnapshot(t, pins)
	selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, SnapshotID: "snap-1", TargetID: "invoice", Parameters: json.RawMessage(`{}`)}
	_, normalized, fingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
	if err != nil {
		t.Fatal(err)
	}
	projects := &selectionProjects{execution: validResolvedSelection(t, selection, fingerprint, storagev1.Principal{Subject: "user-1", Domain: "core"})}
	server, err := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true, Credentials: StaticTokens{"valid-token": {Subject: "user-1", Domain: "core"}}, Access: access, Pins: pins, Projects: projects, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
	if err != nil {
		t.Fatal(err)
	}
	response := postRenderSelection(server, string(normalized), "working-source-read-denied")
	if response.Code != http.StatusForbidden || projects.resolveCall != 0 || len(pins.acquireKeys) != 0 {
		t.Fatalf("working selection crossed source.read boundary: status=%d resolve=%d pins=%v body=%s", response.Code, projects.resolveCall, pins.acquireKeys, response.Body)
	}
	initiator, _ := EncodeInitiator(storagev1.Principal{Subject: "user-1", Domain: "core"})
	if _, found, lookupErr := state.LookupAdmission(context.Background(), selection.Scope.WorkspaceID, initiator, "working-source-read-denied", fingerprint); lookupErr != nil || found {
		t.Fatalf("denied working selection reserved a receipt: found=%v err=%v", found, lookupErr)
	}
}

func TestRenderBundleSelectionCreatesNoAdmissionState(t *testing.T) {
	state := openHTTPStoreTest(t)
	pins := &testPins{}
	configureTestSnapshot(t, pins)
	selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, ReleaseID: "release-bundle"}
	_, normalized, fingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
	if err != nil {
		t.Fatal(err)
	}
	projects := &selectionProjects{execution: projectsv1.ResolvedExecution{Descriptor: projectsv1.TargetDescriptor{Kind: "bundle"}}}
	server, err := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true, Credentials: StaticTokens{"valid-token": {Subject: "user-1", Domain: "core"}}, Access: &testAccess{allowed: true}, Pins: pins, Projects: projects, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
	if err != nil {
		t.Fatal(err)
	}
	response := postRenderSelection(server, string(normalized), "bundle-rejected")
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("bundle status=%d, body=%s", response.Code, response.Body)
	}
	initiator, _ := EncodeInitiator(storagev1.Principal{Subject: "user-1", Domain: "core"})
	if _, found, lookupErr := state.LookupAdmission(context.Background(), selection.Scope.WorkspaceID, initiator, "bundle-rejected", fingerprint); lookupErr != nil || found {
		t.Fatalf("bundle created a receipt: found=%v err=%v", found, lookupErr)
	}
	if pending, pendingErr := state.PendingAdmissions(context.Background(), 10); pendingErr != nil || len(pending) != 0 || len(pins.acquireKeys) != 0 {
		t.Fatalf("bundle reserved work or pins: pending=%v pins=%v err=%v", pending, pins.acquireKeys, pendingErr)
	}
}

func TestRenderSelectionPreservesProjectsFieldPointersAndSafeMessage(t *testing.T) {
	state := openHTTPStoreTest(t)
	pins := &testPins{}
	configureTestSnapshot(t, pins)
	selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, ReleaseID: "release-1"}
	_, normalized, _, err := generationv1.NormalizeSelection(mustJSON(t, selection))
	if err != nil {
		t.Fatal(err)
	}
	projects := &selectionProjects{resolveErr: &projectsv1.HTTPError{Status: http.StatusUnprocessableEntity, Code: "invalid_selection", Message: "private validator detail", Fields: []projectsv1.FieldError{{File: "schema.json", Pointer: "/properties/title", Code: "required", Message: "title is required"}}}}
	server, err := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true, Credentials: StaticTokens{"valid-token": {Subject: "user-1", Domain: "core"}}, Access: &testAccess{allowed: true}, Pins: pins, Projects: projects, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
	if err != nil {
		t.Fatal(err)
	}
	response := postRenderSelection(server, string(normalized), "selection-field-error")
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("Projects validation status=%d body=%s", response.Code, response.Body)
	}
	var body apiError
	if err = json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Message != "The render selection could not be resolved" || len(body.Error.Fields) != 1 || body.Error.Fields[0].File != "schema.json" || body.Error.Fields[0].Pointer != "/properties/title" {
		t.Fatalf("Projects field errors or safe text were lost: %+v", body.Error)
	}
	if strings.Contains(response.Body.String(), "private validator detail") || len(pins.acquireKeys) != 0 {
		t.Fatalf("unsafe dependency text or pin side effect escaped: %s pins=%v", response.Body, pins.acquireKeys)
	}
	initiator, _ := EncodeInitiator(storagev1.Principal{Subject: "user-1", Domain: "core"})
	if _, found, lookupErr := state.LookupAdmission(context.Background(), selection.Scope.WorkspaceID, initiator, "selection-field-error", ""); lookupErr != nil || found {
		t.Fatalf("invalid selection reserved a receipt: found=%v err=%v", found, lookupErr)
	}
}

func postRenderSelection(handler http.Handler, body, key string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/renders", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer valid-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func validResolvedSelection(t *testing.T, selection generationv1.RenderSelectionRequest, fingerprint string, actor storagev1.Principal) projectsv1.ResolvedExecution {
	t.Helper()
	content := []byte("input bytes")
	hash := sha256.Sum256(content)
	digest := hex.EncodeToString(hash[:])
	profile := projectsv1.Profile{Inputs: map[string]string{"font": "serif"}, PDFStandard: "a-2b"}
	prepared := projectsv1.PreparedInput{JSON: json.RawMessage(`{}`), Files: []projectsv1.PreparedFile{}, Inputs: map[string]string{"font": "serif", "leafrun-data": "/input.json"}}
	var resources []projectsv1.InputResource
	if len(selection.FileRefs) > 0 {
		files := make([]projectsv1.PreparedFile, 0, len(selection.FileRefs))
		inputs := map[string]any{}
		for pointer, reference := range selection.FileRefs {
			file := projectsv1.PreparedFile{Pointer: pointer, ResourceID: reference.ResourceID, Filename: reference.Filename, SHA256: digest, SizeBytes: int64(len(content)), Path: generationv1.GeneratedInputPath(digest, reference.Filename)}
			files = append(files, file)
			var tokens []string
			for start := 1; start < len(pointer); start++ {
				if pointer[start] == '/' {
					tokens = append(tokens, pointer[1:start])
				}
			}
			tokens = append(tokens, pointer[strings.LastIndex(pointer, "/")+1:])
			value := any("/" + file.Path)
			for index := len(tokens) - 1; index >= 0; index-- {
				value = map[string]any{tokens[index]: value}
			}
			for key, item := range value.(map[string]any) {
				inputs[key] = item
			}
			resources = append(resources, projectsv1.InputResource{File: file, Scope: selection.Scope, Class: storagev1.ClassResource})
		}
		encoded, err := json.Marshal(inputs)
		if err != nil {
			t.Fatal(err)
		}
		prepared.JSON, err = jsonvalue.NormalizeObject(encoded)
		if err != nil {
			t.Fatal(err)
		}
		prepared.Files = files
	}
	snapshotID := selection.SnapshotID
	if snapshotID == "" {
		snapshotID = "snap-1"
	}
	releaseID := selection.ReleaseID
	return projectsv1.ResolvedExecution{
		Actor: actor, SelectionFingerprint: fingerprint, Scope: selection.Scope, ReleaseID: releaseID,
		Revision: "rev-1", SnapshotID: snapshotID, ContentID: strings.Repeat("a", 64),
		Descriptor: projectsv1.TargetDescriptor{TargetID: "invoice", Kind: "document", Entrypoint: "main.typ", InputSchema: "schema.json", SnapshotContentID: strings.Repeat("a", 64), Digest: strings.Repeat("b", 64), ExecutionDigest: strings.Repeat("c", 64), TypstVersion: "0.15.1", Profiles: map[string]projectsv1.Profile{"production": profile}, DefaultProfile: "production", SelectedProfile: "production", ResolvedProfile: profile},
		Prepared:   prepared, InputResources: resources,
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
