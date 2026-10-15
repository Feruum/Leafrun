package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

const createBody = `{"scope":{"workspaceId":"ws-1","projectId":"project-1"},"snapshotId":"snap-1","targetId":"pdf","entrypoint":"main.typ"}`

type testStore struct {
	mu          sync.Mutex
	job         generationv1.Job
	fingerprint string
	admitCalls  int
	latest      *store.PinAcquire
	expiredKeys []string
	active      bool
	marked      int
}

func newTestStore() *testStore {
	return &testStore{job: generationv1.Job{JobID: "gen_test", Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, Status: generationv1.Queued, SourceSnapshotID: "snap-1", TargetID: "pdf", Entrypoint: "main.typ"}}
}
func (s *testStore) LookupAdmission(_ context.Context, _, _, _, fingerprint string) (generationv1.Admission, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fingerprint == "" {
		return generationv1.Admission{}, false, nil
	}
	if s.fingerprint != fingerprint {
		return generationv1.Admission{}, false, store.ErrConflict
	}
	return generationv1.Admission{Job: s.job}, true, nil
}
func (s *testStore) Admit(_ context.Context, in store.AdmitInput) (generationv1.Admission, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.admitCalls++
	if s.fingerprint != "" {
		if s.fingerprint != in.Fingerprint {
			return generationv1.Admission{}, false, store.ErrConflict
		}
		return generationv1.Admission{Job: s.job}, true, nil
	}
	s.fingerprint = in.Fingerprint
	s.job.InputFingerprint = in.Fingerprint
	return generationv1.Admission{Job: s.job}, false, nil
}
func (s *testStore) GetJob(context.Context, string) (generationv1.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.job, nil
}
func (s *testStore) RequestCancel(context.Context, string) (bool, error) { return true, nil }
func (s *testStore) LatestPinAcquire(_ context.Context, _, _ string) (store.PinAcquire, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil {
		return store.PinAcquire{}, store.ErrNotFound
	}
	copy := *s.latest
	if s.latest.Response != nil {
		pin := *s.latest.Response
		copy.Response = &pin
	}
	return copy, nil
}
func (s *testStore) RecordPinAcquireExpired(_ context.Context, _, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expiredKeys = append(s.expiredKeys, key)
	if s.latest == nil || s.latest.Key != key {
		return store.ErrNotFound
	}
	s.latest.State = "failed"
	return nil
}
func (s *testStore) AdmissionActivated(context.Context, string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active, nil
}
func (s *testStore) BeginPinAcquireIntent(_ context.Context, in store.PinAcquireIntent) (store.PinAcquire, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest != nil && s.latest.State == "pending" {
		return store.PinAcquire{}, store.ErrInvalidTransition
	}
	request, _ := json.Marshal(in.Request)
	item := store.PinAcquire{Number: 1, Key: in.IdempotencyKey, Request: request, State: "pending"}
	s.latest = &item
	return item, nil
}
func (s *testStore) RecordPinAcquireResponse(_ context.Context, _, key string, pin storagev1.Pin) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil || s.latest.Key != key {
		return store.ErrNotFound
	}
	copy := pin
	s.latest.Response = &copy
	return nil
}
func (s *testStore) MarkPinActiveAndRecordWorkflowStart(_ context.Context, jobID, key, workflowID string, pin storagev1.Pin, request []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if jobID != s.job.JobID || s.latest == nil || s.latest.Key != key || workflowID != "render/"+jobID || len(request) == 0 || pin.State != "active" {
		return errors.New("invalid active pin or workflow intent")
	}
	s.active = true
	s.marked++
	return nil
}
func (s *testStore) MarkPinActive(context.Context, string, string, storagev1.Pin) error { return nil }
func (s *testStore) PendingAdmissions(context.Context, int) ([]string, error)           { return nil, nil }
func (s *testStore) ExpireDue(context.Context, int) (int, error)                        { return 0, nil }
func (s *testStore) Retain(context.Context) (store.RetentionResult, error) {
	return store.RetentionResult{}, nil
}

type testAccess struct {
	mu      sync.Mutex
	allowed bool
	calls   []storagev1.AccessCheck
}

func (a *testAccess) Check(_ context.Context, check storagev1.AccessCheck) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, check)
	return a.allowed, nil
}

type testOwner struct{}

func (testOwner) Publishable(context.Context, storagev1.PublishabilityCheck) (bool, error) {
	return false, nil
}
func (testOwner) Pin(context.Context, storagev1.PinOwnerCheck) (storagev1.OwnerState, error) {
	return storagev1.OwnerAbsent, nil
}

type testPins struct {
	mu                sync.Mutex
	acquireKeys       []string
	known             map[string]storagev1.Pin
	loseFirstAck      bool
	expireFirst       bool
	acquireFirstError error
	activateCalls     int
	snapshot          storagev1.Snapshot
	snapshotErr       error
	snapshotCalls     int
}

func (p *testPins) GetSnapshot(_ context.Context, snapshotID string) (storagev1.Snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshotCalls++
	if p.snapshotErr != nil {
		return storagev1.Snapshot{}, p.snapshotErr
	}
	if p.snapshot.SnapshotID != snapshotID {
		return storagev1.Snapshot{}, &HTTPStatusError{StatusCode: http.StatusNotFound}
	}
	return p.snapshot, nil
}

func (p *testPins) AcquirePin(_ context.Context, request storagev1.AcquirePinRequest) (storagev1.Pin, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.acquireKeys = append(p.acquireKeys, request.IdempotencyKey)
	if p.acquireFirstError != nil && len(p.acquireKeys) == 1 {
		return storagev1.Pin{}, p.acquireFirstError
	}
	if p.expireFirst && len(p.acquireKeys) == 1 {
		return storagev1.Pin{}, &HTTPStatusError{StatusCode: http.StatusConflict, Code: "pin_expired"}
	}
	if pin, ok := p.known[request.IdempotencyKey]; ok {
		return pin, nil
	}
	pin := storagev1.Pin{PinID: "pin_" + request.IdempotencyKey, Scope: request.Scope, ObjectID: request.ObjectID, HolderID: request.HolderID, HolderDomain: request.HolderDomain, RetentionPolicy: request.RetentionPolicy, State: "pending", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	if p.known == nil {
		p.known = map[string]storagev1.Pin{}
	}
	p.known[request.IdempotencyKey] = pin
	if p.loseFirstAck && len(p.acquireKeys) == 1 {
		return storagev1.Pin{}, errors.New("simulated lost Storage acknowledgement")
	}
	return pin, nil
}
func (p *testPins) GetPin(_ context.Context, pinID string) (storagev1.Pin, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pin := range p.known {
		if pin.PinID == pinID {
			return pin, nil
		}
	}
	return storagev1.Pin{}, &HTTPStatusError{StatusCode: http.StatusGone}
}
func (p *testPins) ActivatePin(_ context.Context, pinID string) (storagev1.Pin, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.activateCalls++
	for key, pin := range p.known {
		if pin.PinID == pinID {
			if !time.Now().Before(pin.ExpiresAt) {
				return storagev1.Pin{}, &HTTPStatusError{StatusCode: http.StatusConflict, Code: "pin_expired"}
			}
			pin.State = "active"
			p.known[key] = pin
			return pin, nil
		}
	}
	return storagev1.Pin{}, errors.New("pin not found")
}

func newTestServer(t *testing.T, s *testStore, access *testAccess, pins *testPins) *Server {
	t.Helper()
	configureTestSnapshot(t, pins)
	server, err := New(s, testOwner{}, Config{
		Credentials: StaticTokens{"valid-token": {Subject: "user-1", Domain: "core"}},
		Access:      access, Pins: pins, OwnerToken: "owner-secret", EngineVersion: "0.15.1",
		Ready: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func configureTestSnapshot(t *testing.T, pins *testPins) {
	t.Helper()
	if pins.snapshot.SnapshotID == "" {
		manifest := storagev1.SnapshotManifest{
			FormatVersion: 1,
			Files:         []storagev1.SnapshotFile{{Path: "main.typ", Size: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
			Directories:   []string{}, Fonts: []storagev1.SnapshotFont{},
		}
		id, err := storagev1.ManifestContentID(manifest)
		if err != nil {
			t.Fatal(err)
		}
		manifest.ID = id
		pins.snapshot = storagev1.Snapshot{SnapshotID: "snap-1", Scope: storagev1.Scope{WorkspaceID: "ws-1", ProjectID: "project-1"}, Manifest: manifest}
	}
}

func createRequest(handler http.Handler, body string, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func TestCreateReturnsAcceptedOnlyAfterPinAndWorkflowIntentAndRechecksReplayAccess(t *testing.T) {
	s := newTestStore()
	access := &testAccess{allowed: true}
	pins := &testPins{}
	handler := newTestServer(t, s, access, pins)

	first := createRequest(handler, createBody, "external-key")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first POST status = %d, body %s", first.Code, first.Body)
	}
	if !s.active || s.marked != 1 || pins.activateCalls != 1 {
		t.Fatalf("response preceded activation/intent: active=%v marked=%d activations=%d", s.active, s.marked, pins.activateCalls)
	}
	var admission generationv1.Admission
	if err := json.Unmarshal(first.Body.Bytes(), &admission); err != nil || admission.Job.JobID != s.job.JobID {
		t.Fatalf("invalid admission response: %#v, %v", admission, err)
	}

	access.mu.Lock()
	access.allowed = false
	access.mu.Unlock()
	second := createRequest(handler, createBody, "external-key")
	if second.Code != http.StatusForbidden {
		t.Fatalf("replay after rights revocation status = %d, body %s", second.Code, second.Body)
	}
	if s.admitCalls != 1 {
		t.Fatalf("denied replay reached Store.Admit %d times", s.admitCalls-1)
	}
}

func TestCreateKeepsSamePinKeyWhenAcquireAcknowledgementIsLost(t *testing.T) {
	s := newTestStore()
	access := &testAccess{allowed: true}
	pins := &testPins{loseFirstAck: true}
	handler := newTestServer(t, s, access, pins)

	first := createRequest(handler, createBody, "external-key")
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("first POST status = %d, want dependency retry, body %s", first.Code, first.Body)
	}
	second := createRequest(handler, createBody, "external-key")
	if second.Code != http.StatusAccepted {
		t.Fatalf("replay POST status = %d, body %s", second.Code, second.Body)
	}
	if len(pins.acquireKeys) != 2 || pins.acquireKeys[0] != pins.acquireKeys[1] {
		t.Fatalf("lost AcquirePin acknowledgement did not reuse persisted key: %v", pins.acquireKeys)
	}
}

func TestCreateRotatesStorageKeyOnlyAfterTypedPinExpiry(t *testing.T) {
	s := newTestStore()
	access := &testAccess{allowed: true}
	pins := &testPins{expireFirst: true}
	handler := newTestServer(t, s, access, pins)

	response := createRequest(handler, createBody, "external-key")
	if response.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d, body %s", response.Code, response.Body)
	}
	if len(pins.acquireKeys) != 2 || pins.acquireKeys[0] == pins.acquireKeys[1] {
		t.Fatalf("typed pin expiry did not create a new durable Storage key: %v", pins.acquireKeys)
	}
	if len(s.expiredKeys) != 1 || s.expiredKeys[0] != pins.acquireKeys[0] {
		t.Fatalf("typed pin expiry was not durably recorded before rotation: expired=%v keys=%v", s.expiredKeys, pins.acquireKeys)
	}
}

func TestRecoveredExpiredPendingPinRotatesForSourceAndDerivedSnapshots(t *testing.T) {
	for _, source := range []bool{true, false} {
		t.Run(fmt.Sprint(source), func(t *testing.T) {
			state := newTestStore()
			objectID := state.job.SourceSnapshotID
			if !source {
				objectID = "snap-derived"
			}
			request := storagev1.AcquirePinRequest{Scope: state.job.Scope, ObjectID: objectID, HolderID: "leafrun-generation", HolderDomain: "service", RetentionPolicy: "ordinary", IdempotencyKey: "old-key"}
			pin := storagev1.Pin{PinID: "pin-old", Scope: request.Scope, ObjectID: objectID, HolderID: request.HolderID, HolderDomain: request.HolderDomain, RetentionPolicy: request.RetentionPolicy, State: "pending", ExpiresAt: time.Now().Add(-time.Minute)}
			encoded, _ := json.Marshal(request)
			state.latest = &store.PinAcquire{Key: request.IdempotencyKey, Request: encoded, Response: &pin, State: "pending"}
			pins := &testPins{known: map[string]storagev1.Pin{request.IdempotencyKey: pin}}
			server := newTestServer(t, state, &testAccess{allowed: true}, pins)
			var err error
			if source {
				err = server.EnsureSourcePin(context.Background(), state.job)
			} else {
				err = server.EnsureDerivedPin(context.Background(), state.job, objectID)
			}
			if err != nil {
				t.Fatalf("expired pending pin blocked recovery: %v", err)
			}
			if len(state.expiredKeys) != 1 || state.expiredKeys[0] != "old-key" || len(pins.acquireKeys) != 1 || pins.acquireKeys[0] == "old-key" || pins.activateCalls != 2 {
				t.Fatalf("rotation did not persist expiry and activate a fresh key: expired=%v keys=%v activations=%d", state.expiredKeys, pins.acquireKeys, pins.activateCalls)
			}
		})
	}
}

func TestPinExpiryUsesStorageClockInsteadOfAPISystemClock(t *testing.T) {
	for _, source := range []bool{true, false} {
		t.Run(fmt.Sprintf("source=%v", source), func(t *testing.T) {
			state := newTestStore()
			objectID := state.job.SourceSnapshotID
			if !source {
				objectID = "snap-derived"
				state.job.Status = generationv1.Preparing
				state.job.DerivedSnapshotID = objectID
			}
			pins := &testPins{}
			apiNow := time.Now().Add(2 * time.Hour)
			server, err := New(state, testOwner{}, Config{
				Credentials: StaticTokens{"valid-token": {Subject: "user-1", Domain: "core"}},
				Access:      &testAccess{allowed: true}, Pins: pins, OwnerToken: "owner-secret", EngineVersion: "0.15.1",
				Now: func() time.Time { return apiNow },
			})
			if err != nil {
				t.Fatal(err)
			}

			if source {
				err = server.EnsureSourcePin(context.Background(), state.job)
			} else {
				err = server.EnsureDerivedPin(context.Background(), state.job, objectID)
			}
			if err != nil {
				t.Fatalf("live Storage pin was rejected using the API clock: %v", err)
			}
			if len(state.expiredKeys) != 0 || len(pins.acquireKeys) != 1 || pins.activateCalls != 1 {
				t.Fatalf("live pin was expired or rotated: expired=%v keys=%v activations=%d", state.expiredKeys, pins.acquireKeys, pins.activateCalls)
			}
			if source && (!state.active || state.marked != 1) {
				t.Fatalf("source pin/workflow intent was not activated: active=%v marked=%d", state.active, state.marked)
			}
		})
	}
}

type conflictPins struct {
	*testPins
	acquireConflict, activateConflict bool
	code                              string
}

type activationResponsePins struct {
	*testPins
	responseErr error
}

func (p *activationResponsePins) ActivatePin(ctx context.Context, id string) (storagev1.Pin, error) {
	_, err := p.testPins.ActivatePin(ctx, id)
	if err != nil {
		return storagev1.Pin{}, err
	}
	return storagev1.Pin{}, p.responseErr
}

func TestPinKnownActivationRejectionCannotBeOverriddenByActiveMetadata(t *testing.T) {
	for _, source := range []bool{true, false} {
		for _, tc := range []struct {
			name   string
			err    error
			accept bool
		}{
			{"owner_absent", &HTTPStatusError{StatusCode: http.StatusConflict, Code: "owner_absent"}, false},
			{"forbidden", &HTTPStatusError{StatusCode: http.StatusForbidden}, false},
			{"owner_unknown", &HTTPStatusError{StatusCode: http.StatusServiceUnavailable}, false},
			{"lost_response", errors.New("lost activation reply"), true},
		} {
			t.Run(fmt.Sprintf("source=%v/%s", source, tc.name), func(t *testing.T) {
				state := newTestStore()
				objectID := state.job.SourceSnapshotID
				if !source {
					objectID = "snap-derived"
					state.job.Status = generationv1.Preparing
					state.job.DerivedSnapshotID = objectID
				}
				pins := &activationResponsePins{testPins: &testPins{}, responseErr: tc.err}
				server, err := New(state, testOwner{}, Config{
					Credentials: StaticTokens{"valid-token": {Subject: "user-1", Domain: "core"}},
					Access:      &testAccess{allowed: true}, Pins: pins, OwnerToken: "owner-secret", EngineVersion: "0.15.1",
				})
				if err != nil {
					t.Fatal(err)
				}
				if source {
					err = server.EnsureSourcePin(context.Background(), state.job)
				} else {
					err = server.EnsureDerivedPin(context.Background(), state.job, objectID)
				}
				if (err == nil) != tc.accept {
					t.Fatalf("accept=%v: active metadata overrode a definitive rejection or lost reply recovery failed: %v", tc.accept, err)
				}
				if len(state.expiredKeys) != 0 || len(pins.acquireKeys) != 1 || !tc.accept && state.marked != 0 {
					t.Fatalf("rejection changed durable pin state: expired=%v keys=%v marked=%d", state.expiredKeys, pins.acquireKeys, state.marked)
				}
			})
		}
	}
}

func (p *conflictPins) AcquirePin(ctx context.Context, request storagev1.AcquirePinRequest) (storagev1.Pin, error) {
	pin, err := p.testPins.AcquirePin(ctx, request)
	if p.acquireConflict && len(p.acquireKeys) == 1 {
		return storagev1.Pin{}, &HTTPStatusError{StatusCode: http.StatusConflict, Code: p.code}
	}
	return pin, err
}

func (p *conflictPins) ActivatePin(ctx context.Context, id string) (storagev1.Pin, error) {
	if p.activateConflict && len(p.acquireKeys) == 1 {
		return storagev1.Pin{}, &HTTPStatusError{StatusCode: http.StatusConflict, Code: p.code}
	}
	return p.testPins.ActivatePin(ctx, id)
}

func TestPinExpiredConflictRotatesAcquireAndActivationWithoutRotatingOtherConflicts(t *testing.T) {
	for _, operation := range []string{"acquire", "activate"} {
		for _, code := range []string{"pin_expired", "owner_absent"} {
			t.Run(operation+"/"+code, func(t *testing.T) {
				state, pins := newTestStore(), &testPins{}
				server := newTestServer(t, state, &testAccess{allowed: true}, pins)
				server.cfg.Pins = &conflictPins{testPins: pins, acquireConflict: operation == "acquire", activateConflict: operation == "activate", code: code}
				err := server.EnsureSourcePin(context.Background(), state.job)
				if code == "pin_expired" {
					if err != nil || len(state.expiredKeys) != 1 || len(pins.acquireKeys) != 2 || pins.acquireKeys[0] == pins.acquireKeys[1] || !state.active {
						t.Fatalf("authoritative pin expiry did not rotate and activate: %v expired=%v keys=%v", err, state.expiredKeys, pins.acquireKeys)
					}
				} else if err == nil || len(state.expiredKeys) != 0 || len(pins.acquireKeys) != 1 {
					t.Fatalf("unrelated conflict rotated a durable key: %v expired=%v keys=%v", err, state.expiredKeys, pins.acquireKeys)
				}
			})
		}
	}
}

func TestPinClientRetainsOnlyAllowlistedExpiryCodeAndNeverPrivateText(t *testing.T) {
	for _, code := range []string{"pin_expired", "owner_absent"} {
		t.Run(code, func(t *testing.T) {
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				fmt.Fprintf(w, `{"error":{"code":%q,"message":"private dependency detail"}}`, code)
			}))
			defer endpoint.Close()
			client, err := NewPinClient(endpoint.URL, "service-token", endpoint.Client())
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.ActivatePin(context.Background(), "pin-1")
			var failure *HTTPStatusError
			if !errors.As(err, &failure) || failure.StatusCode != http.StatusConflict || isExpiredPinError(err) != (code == "pin_expired") || strings.Contains(err.Error(), "private") {
				t.Fatalf("invalid expiry classification or error text: %v", err)
			}
		})
	}
}

func TestCreateRejectsMissingSnapshotBeforeAdmission(t *testing.T) {
	s := newTestStore()
	access := &testAccess{allowed: true}
	pins := &testPins{snapshotErr: &HTTPStatusError{StatusCode: http.StatusNotFound}}
	handler := newTestServer(t, s, access, pins)

	response := createRequest(handler, createBody, "external-key")
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing snapshot status = %d, body %s", response.Code, response.Body)
	}
	if s.admitCalls != 0 || len(pins.acquireKeys) != 0 {
		t.Fatalf("missing snapshot reserved admission or pin: admit=%d pins=%v", s.admitCalls, pins.acquireKeys)
	}
}

func TestCreateChecksEntrypointBeforeAdmission(t *testing.T) {
	s := newTestStore()
	access := &testAccess{allowed: true}
	pins := &testPins{}
	handler := newTestServer(t, s, access, pins)

	response := createRequest(handler, `{"scope":{"workspaceId":"ws-1","projectId":"project-1"},"snapshotId":"snap-1","targetId":"pdf","entrypoint":"missing.typ"}`, "external-key")
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("missing entrypoint status = %d, body %s", response.Code, response.Body)
	}
	if s.admitCalls != 0 || len(pins.acquireKeys) != 0 {
		t.Fatalf("missing entrypoint reserved admission or pin: admit=%d pins=%v", s.admitCalls, pins.acquireKeys)
	}
}

func TestCreateReplayUsesReceiptWhenSourceSnapshotHasExpired(t *testing.T) {
	s := newTestStore()
	access := &testAccess{allowed: true}
	pins := &testPins{}
	handler := newTestServer(t, s, access, pins)

	first := createRequest(handler, createBody, "external-key")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first POST status = %d, body %s", first.Code, first.Body)
	}
	firstSnapshotReads := pins.snapshotCalls
	pins.mu.Lock()
	pins.snapshotErr = &HTTPStatusError{StatusCode: http.StatusGone}
	pins.mu.Unlock()

	replay := createRequest(handler, createBody, "external-key")
	if replay.Code != http.StatusAccepted {
		t.Fatalf("replay after source expiry status = %d, body %s", replay.Code, replay.Body)
	}
	if pins.snapshotCalls != firstSnapshotReads {
		t.Fatalf("replay looked up source again: reads before=%d after=%d", firstSnapshotReads, pins.snapshotCalls)
	}
}
