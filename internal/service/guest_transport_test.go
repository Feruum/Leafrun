package service

import (
	"context"
	"encoding/json"
	"fmt"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

var _ = context.Background

func newGuestResolveEndpoint(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var in accessv1.ResolveRequest
		if json.NewDecoder(r.Body).Decode(&in) != nil || in.Validate() != nil || r.Header.Get("Authorization") != "Bearer fixed-service" || r.Header.Get("Cookie") != "" {
			t.Error("resolver transport mismatch")
		}
		if in.SessionCookie == "stale" {
			w.WriteHeader(401)
			return
		}
		if in.SessionCookie != "opaque-guest" || in.SessionCookieKind != accessv1.CookieKindGuest {
			t.Errorf("wrong guest selection: %v", in)
		}
		fmt.Fprint(w, `{"identity":{"principal":{"subject":"grant","domain":"guest"},"authority":{"kind":"guest_session","id":"exact-session","version":1}},"expiresAt":"2099-01-01T00:00:00Z"}`)
	}))
	return s, calls
}

func TestGuestTransportUsesSharedResolverAndExactAuthority(t *testing.T) {
	s, calls := newGuestResolveEndpoint(t)
	defer s.Close()
	names := authn.CookieNames{Human: "custom_human", Guest: "custom_guest"}
	client, err := authn.NewClientWithCookieNames(s.URL, "fixed-service", names, nil)
	if err != nil {
		t.Fatal(err)
	}
	guard, _ := authn.NewBrowserGuard([]string{"https://app.example"}, false)
	state := newTestStore()
	checks := &testAccess{allowed: true}
	server, err := New(boundTestStore{state}, testOwner{}, Config{AuthMode: "access", Authenticator: client, CookieName: "custom_human", GuestCookieName: "custom_guest", BrowserGuard: guard, Access: checks, Pins: &testPins{}, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "https://app.example/v1/jobs/gen_test/cancel", nil)
	r.Header.Set("Cookie", "unrelated=ignored; custom_guest=opaque-guest")
	r.Header.Set("Origin", "https://app.example")
	w := httptest.NewRecorder()
	server.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("guest cancel=%d %s", w.Code, w.Body)
	}
	if len(checks.calls) == 0 || checks.calls[0].Authority == nil || checks.calls[0].Authority.ID != "exact-session" || checks.calls[0].Principal.Domain != "guest" {
		t.Fatal("guest authority lost before policy")
	}
	if calls.Load() != 1 {
		t.Fatal("expected one resolver call")
	}
	for _, headers := range []http.Header{{"Cookie": {"custom_human=human; custom_guest=opaque-guest"}}, {"Cookie": {"custom_guest=opaque-guest"}, "Authorization": {"Bearer caller"}}, {"Cookie": {"custom_guest=x; custom_guest=y"}}, {"Cookie": {"custom_guest="}}} {
		r := httptest.NewRequest("POST", "https://app.example/v1/jobs/gen_test/cancel", nil)
		r.Header = headers
		r.Header.Set("Origin", "https://app.example")
		w := httptest.NewRecorder()
		server.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("ambiguous=%d", w.Code)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("ambiguous candidate reached resolver")
	}
}
