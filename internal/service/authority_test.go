package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Feruum/Leafrun/internal/store"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

type publicAuthenticator struct{ err error }
type boundTestStore struct{ *testStore }

func (s boundTestStore) LoadAuthority(context.Context, string) (store.AuthorityBinding, error) {
	principal := storagev1.Principal{Subject: "alice", Domain: "user"}
	initiator, _ := EncodeInitiator(principal)
	return store.AuthorityBinding{JobID: s.job.JobID, Scope: s.job.Scope, InitiatorID: initiator}, nil
}

func (a publicAuthenticator) Authenticate(context.Context, *http.Request) (identityv1.Identity, error) {
	return identityv1.Identity{Principal: storagev1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session-1", Version: 1}}, a.err
}

func TestOwnerRequiresExplicitAuthenticationMode(t *testing.T) {
	cfg := Config{Credentials: StaticTokens{"valid-token": {Subject: "alice", Domain: "user"}}, Access: &testAccess{allowed: true}, Pins: &testPins{}, OwnerToken: "owner-secret", EngineVersion: "0.15.1"}
	if _, err := New(newTestStore(), testOwner{}, cfg); err == nil {
		t.Fatal("implicit fixture mode accepted")
	}
	cfg.AuthMode = "dev"
	if _, err := New(newTestStore(), testOwner{}, cfg); err == nil {
		t.Fatal("dev fixture gate omitted")
	}
	cfg.AuthMode = "access"
	cfg.DevFixtures = true
	if _, err := New(newTestStore(), testOwner{}, cfg); err == nil {
		t.Fatal("access fell back to fixture")
	}
}

func TestAccessAuthenticationAndCookieCancelFailClosed(t *testing.T) {
	guard, _ := authn.NewBrowserGuard([]string{"https://app.example"}, false)
	for _, tc := range []struct {
		name           string
		err            error
		cookie, origin string
		status         int
	}{
		{"provider outage", accessv1.ErrDependency, "", "", 503},
		{"expired", accessv1.ErrUnauthenticated, "", "", 401},
		{"missing cookie origin", nil, "ory_kratos_session=secret", "", 403},
		{"foreign cookie origin", nil, "ory_kratos_session=secret", "https://evil.example", 403},
		{"guest missing origin", nil, "leafrun_guest_session=guest", "", 403},
		{"both cookies", nil, "ory_kratos_session=human; leafrun_guest_session=guest", "https://app.example", 401},
		{"guest cookie", nil, "leafrun_guest_session=guest", "https://app.example", 200},
		{"valid cookie", nil, "ory_kratos_session=secret", "https://app.example", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestStore()
			access := &testAccess{allowed: true}
			server, err := New(boundTestStore{state}, testOwner{}, Config{AuthMode: "access", Authenticator: publicAuthenticator{tc.err}, BrowserGuard: guard, Access: access, Pins: &testPins{}, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "/v1/jobs/gen_test/cancel", nil)
			if tc.cookie != "" {
				r.Header.Set("Cookie", tc.cookie)
			} else {
				r.Header.Set("Authorization", "Bearer token")
			}
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			w := httptest.NewRecorder()
			server.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			if tc.status != 200 && len(access.calls) != 0 {
				t.Fatal("denied request checked domain rights")
			}
			if tc.status == 200 && (len(access.calls) == 0 || access.calls[0].Authority == nil || access.calls[0].Authority.ID != "session-1") {
				t.Fatal("verified authority missing from policy check")
			}
		})
	}
}
