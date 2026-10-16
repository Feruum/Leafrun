package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Feruum/Leafrun/internal/store"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

type e06DisclosureStore struct {
	*testStore
	loadErr error
}

func (s e06DisclosureStore) LoadAuthority(context.Context, string) (store.AuthorityBinding, error) {
	initiator, _ := EncodeInitiator(storagev1.Principal{Subject: "alice", Domain: "user"})
	return store.AuthorityBinding{JobID: s.job.JobID, Scope: s.job.Scope, InitiatorID: initiator, Resources: []store.AuthorityResource{{Scope: s.job.Scope, ResourceID: "selected-a"}, {Scope: s.job.Scope, ResourceID: "selected-b"}}}, s.loadErr
}

type e06DisclosureAccess struct {
	deny, errorObject string
	selected          []string
}

func (a *e06DisclosureAccess) Check(_ context.Context, c storagev1.AccessCheck) (bool, error) {
	if c.Operation == "resource.read" {
		a.selected = append(a.selected, c.ObjectID)
		if c.ObjectID == a.errorObject {
			return false, errors.New("provider unavailable")
		}
		if c.ObjectID == a.deny {
			return false, nil
		}
	}
	return true, nil
}
func TestE06SelectedResourceDisclosure(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, kind := range []string{"deny-second-resource", "resource-provider-outage", "binding-unavailable", "all-current-rights"} {
			t.Run(method+"/"+kind, func(t *testing.T) {
				state := newTestStore()
				state.job.Entrypoint = "secret-entry.typ"
				state.job.RawDiagnostics = "secret-raw"
				state.job.Parameters = json.RawMessage(`{"selected":"/.leafrun-input/secret-resource-hash.svg"}`)
				if err := json.Unmarshal([]byte(`[{"severity":"error","message":"secret-message","hints":["secret-hint"]}]`), &state.job.Diagnostics); err != nil {
					t.Fatal(err)
				}
				wrapped := e06DisclosureStore{testStore: state}
				access := &e06DisclosureAccess{}
				expected := 200
				switch kind {
				case "deny-second-resource":
					access.deny = "selected-b"
				case "resource-provider-outage":
					access.errorObject = "selected-b"
					expected = 503
				case "binding-unavailable":
					wrapped.loadErr = errors.New("binding unavailable")
					expected = 503
				}
				guard, _ := authn.NewBrowserGuard([]string{"https://app.example"}, false)
				server, err := New(wrapped, testOwner{}, Config{AuthMode: "access", Authenticator: publicAuthenticator{}, BrowserGuard: guard, Access: access, Pins: &testPins{}, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
				if err != nil {
					t.Fatal(err)
				}
				path := "/v1/jobs/gen_test"
				if method == http.MethodPost {
					path += "/cancel"
				}
				request := httptest.NewRequest(method, path, nil)
				request.Header.Set("Authorization", "Bearer current-reader")
				response := httptest.NewRecorder()
				server.ServeHTTP(response, request)
				if response.Code != expected {
					t.Fatalf("status=%d want=%d: %s", response.Code, expected, response.Body)
				}
				if kind != "all-current-rights" && strings.Contains(response.Body.String(), "secret-") {
					t.Fatalf("selected resource contents disclosed: %s", response.Body)
				}
				if kind == "deny-second-resource" && !strings.Contains(response.Body.String(), `"diagnosticsVisibility":"restricted"`) {
					t.Fatalf("restricted marker absent: %s", response.Body)
				}
				if kind == "all-current-rights" && (!strings.Contains(response.Body.String(), "secret-resource-hash") || len(access.selected) != 2) {
					t.Fatalf("positive control lost permitted details or skipped resource: %s checks=%v", response.Body, access.selected)
				}
			})
		}
	}
}
