package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/leafrun/leafrun-platform/authn"
)

// The controlled resolver supplies a valid human identity for any selected
// credential. Known local protocol secrets must still never enter public auth.
func TestE06OwnerProtocolSecretNeverPublic(t *testing.T) {
	for _, transport := range []string{"bearer", "cookie"} {
		t.Run(transport, func(t *testing.T) {
			guard, err := authn.NewBrowserGuard([]string{"https://app.example"}, false)
			if err != nil {
				t.Fatal(err)
			}
			access := &testAccess{allowed: true}
			server, err := New(newTestStore(), testOwner{}, Config{AuthMode: "access", Authenticator: publicAuthenticator{}, BrowserGuard: guard, Access: access, Pins: &testPins{}, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "/v1/jobs/gen_test/cancel", nil)
			if transport == "bearer" {
				r.Header.Set("Authorization", "Bearer owner-secret")
			} else {
				r.Header.Set("Cookie", "ory_kratos_session=owner-secret")
				r.Header.Set("Origin", "https://app.example")
			}
			w := httptest.NewRecorder()
			server.ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized || len(access.calls) != 0 {
				t.Fatalf("local protocol credential reached public path: status=%d policy_calls=%d", w.Code, len(access.calls))
			}
		})
	}
}

func TestInternalOwnerBearerRejectsEitherSelectedCookie(t *testing.T) {
	server := &Server{cfg: Config{OwnerToken: "owner-secret", CookieName: "custom_human", GuestCookieName: "custom_guest"}}
	for _, cookie := range []string{"custom_guest=guest", "custom_human=human", "custom_guest=", "custom_guest=a; custom_guest=b"} {
		r := httptest.NewRequest("POST", "http://internal.example/internal/v1/checks/publishable", nil)
		r.Header.Set("Authorization", "Bearer owner-secret")
		r.Header.Set("Cookie", cookie)
		if server.authenticateOwner(r) {
			t.Fatal("owner bearer accepted with selected cookie")
		}
	}
}
