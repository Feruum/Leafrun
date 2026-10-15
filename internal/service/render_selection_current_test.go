package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

type pausedSelectionResources struct {
	resource storagev1.Resource
	entered  chan struct{}
	resume   chan struct{}
}

func (p *pausedSelectionResources) GetResource(ctx context.Context, _ string) (storagev1.Resource, error) {
	close(p.entered)
	select {
	case <-p.resume:
		return p.resource, nil
	case <-ctx.Done():
		return storagev1.Resource{}, ctx.Err()
	}
}

func TestRenderSelectionRechecksCurrentReleaseAfterResourcePreflight(t *testing.T) {
	for _, test := range []struct {
		name     string
		decision projectsv1.CurrentReleaseDecision
		err      error
		status   int
	}{
		{name: "withdrawn", status: http.StatusConflict},
		{name: "rights_revoked", status: http.StatusConflict},
		{name: "outage", decision: projectsv1.CurrentReleaseDecision{Allowed: true}, err: errors.New("private Projects outage detail"), status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, pool := openHTTPStoreTestWithPool(t)
			principal := storagev1.Principal{Subject: "user-1", Domain: "core"}
			selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, ReleaseID: "release-1", FileRefs: map[string]generationv1.FileReference{
				"/logo": {ResourceID: "resource-file", Filename: "logo.svg"},
			}}
			_, normalized, fingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
			if err != nil {
				t.Fatal(err)
			}
			resolved := validResolvedSelection(t, selection, fingerprint, principal)
			file := resolved.Prepared.Files[0]
			resources := &pausedSelectionResources{resource: storagev1.Resource{ResourceID: file.ResourceID, Scope: selection.Scope, Class: storagev1.ClassResource, SHA256: file.SHA256, SizeBytes: file.SizeBytes}, entered: make(chan struct{}), resume: make(chan struct{})}
			projects := &selectionProjects{execution: resolved, current: projectsv1.CurrentReleaseDecision{Allowed: true}}
			pins := &testPins{}
			configureTestSnapshot(t, pins)
			server, err := New(state, state.Owner(), Config{Credentials: StaticTokens{"valid-token": principal}, Access: &testAccess{allowed: true}, Pins: pins, Resources: resources, Projects: projects, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- postRenderSelection(server, string(normalized), "current-release-preflight") }()
			select {
			case <-resources.entered:
			case <-time.After(10 * time.Second):
				close(resources.resume)
				t.Fatal("resource preflight was not reached")
			}
			// Resolution has succeeded, but metadata is still in flight. Withdraw
			// publication, revoke rights, or lose Projects before allowing admission.
			resolveCalls, prematureChecks := projects.resolveCall, len(projects.currentChecks)
			projects.current, projects.currentErr = test.decision, test.err
			close(resources.resume)
			var response *httptest.ResponseRecorder
			select {
			case response = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("render request did not finish after metadata resumed")
			}
			if resolveCalls != 1 || prematureChecks != 0 {
				t.Errorf("check order before metadata completion: resolves=%d current checks=%d", resolveCalls, prematureChecks)
			}
			if response.Code != test.status {
				t.Errorf("current release status=%d, want %d; body=%s", response.Code, test.status, response.Body)
			}
			wantCheck := projectsv1.CurrentReleaseCheck{Actor: principal, Scope: resolved.Scope, ReleaseID: resolved.ReleaseID, DescriptorDigest: resolved.Descriptor.Digest}
			if len(projects.currentChecks) != 1 || projects.currentChecks[0] != wantCheck {
				t.Errorf("current release checks=%+v, want [%+v]", projects.currentChecks, wantCheck)
			}
			for _, table := range []string{"generation_jobs", "generation_admission_receipts", "generation_reservations", "generation_input_resources", "generation_pin_acquires", "generation_workflow_starts"} {
				var count int
				if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Errorf("denied admission left %d rows in %s", count, table)
				}
			}
			if len(pins.acquireKeys) != 0 || pins.activateCalls != 0 {
				t.Errorf("denied admission called Storage pins: acquisitions=%v activations=%d", pins.acquireKeys, pins.activateCalls)
			}
			if strings.Contains(response.Body.String(), "private Projects") {
				t.Error("dependency error detail escaped in public response")
			}
		})
	}
}

func TestRenderSelectionCurrentReleaseAdmissionAndReplay(t *testing.T) {
	for _, test := range []struct {
		name         string
		published    bool
		withResource bool
	}{
		{name: "published_with_resource", published: true, withResource: true},
		{name: "published_without_resources", published: true},
		{name: "working_with_resource", withResource: true},
		{name: "working_without_resources"},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := openHTTPStoreTest(t)
			principal := storagev1.Principal{Subject: "user-1", Domain: "core"}
			selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}}
			if test.published {
				selection.ReleaseID = "release-1"
			} else {
				selection.SnapshotID, selection.TargetID = "snap-1", "invoice"
			}
			if test.withResource {
				selection.FileRefs = map[string]generationv1.FileReference{"/logo": {ResourceID: "resource-file", Filename: "logo.svg"}}
			}
			_, normalized, fingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
			if err != nil {
				t.Fatal(err)
			}
			resolved := validResolvedSelection(t, selection, fingerprint, principal)
			projects := &selectionProjects{execution: resolved}
			if test.published {
				projects.current = projectsv1.CurrentReleaseDecision{Allowed: true}
			} else {
				projects.currentErr = errors.New("working selection must not check current release")
			}
			resources := selectionResources{}
			if test.withResource {
				file := resolved.Prepared.Files[0]
				resources[file.ResourceID] = storagev1.Resource{ResourceID: file.ResourceID, Scope: selection.Scope, Class: storagev1.ClassResource, SHA256: file.SHA256, SizeBytes: file.SizeBytes}
			}
			pins := &testPins{}
			configureTestSnapshot(t, pins)
			server, err := New(state, state.Owner(), Config{Credentials: StaticTokens{"valid-token": principal}, Access: &testAccess{allowed: true}, Pins: pins, Resources: resources, Projects: projects, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
			if err != nil {
				t.Fatal(err)
			}
			first := postRenderSelection(server, string(normalized), "current-release-success")
			if first.Code != http.StatusAccepted {
				t.Fatalf("initial admission status=%d body=%s", first.Code, first.Body)
			}
			wantChecks := 0
			if test.published {
				wantChecks = 1
			}
			if len(projects.currentChecks) != wantChecks {
				t.Errorf("initial current checks=%d, want %d", len(projects.currentChecks), wantChecks)
			}
			initiator, _ := EncodeInitiator(principal)
			admission, found, err := state.LookupAdmission(context.Background(), selection.Scope.WorkspaceID, initiator, "current-release-success", fingerprint)
			if err != nil || !found {
				t.Fatalf("saved admission: found=%v err=%v", found, err)
			}
			input, err := state.GetExecutionInput(context.Background(), admission.Job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(input.Execution, &resolved) {
				t.Errorf("admission changed resolved execution: got=%+v want=%+v", input.Execution, resolved)
			}
			acquires, activations := len(pins.acquireKeys), pins.activateCalls
			projects.resolveErr = errors.New("replay must not resolve")
			projects.current = projectsv1.CurrentReleaseDecision{}
			projects.currentErr = errors.New("replay must not recheck withdrawn release")
			replay := postRenderSelection(server, string(normalized), "current-release-success")
			var replayed generationv1.Admission
			if err := json.Unmarshal(replay.Body.Bytes(), &replayed); err != nil {
				t.Fatal(err)
			}
			if replay.Code != http.StatusAccepted || replayed.Job.JobID != admission.Job.JobID || !replayed.IdempotencyExpiresAt.Equal(admission.IdempotencyExpiresAt) {
				t.Errorf("withdrawn release receipt replay changed: status=%d body=%s original=%s", replay.Code, replay.Body, first.Body)
			}
			if projects.resolveCall != 1 || len(projects.currentChecks) != wantChecks || len(pins.acquireKeys) != acquires || pins.activateCalls != activations {
				t.Errorf("receipt replay repeated dependencies: resolves=%d checks=%d acquisitions=%v activations=%d", projects.resolveCall, len(projects.currentChecks), pins.acquireKeys, pins.activateCalls)
			}
		})
	}
}
