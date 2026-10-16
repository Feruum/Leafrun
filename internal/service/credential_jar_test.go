package service

import (
	"context"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestFixedDependenciesDoNotSendAmbientHumanCookies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "" {
			t.Error("ambient human cookie reached fixed service dependency")
		}
		if r.Header.Get("Authorization") != "Bearer fixed-service-test-token" {
			t.Error("fixed bearer missing")
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse(server.URL)
	jar.SetCookies(u, []*http.Cookie{{Name: "ory_kratos_session", Value: "synthetic-human", Path: "/"}})
	transport := &http.Client{Jar: jar}
	access, err := NewAccessClient(server.URL, "fixed-service-test-token", transport)
	if err != nil {
		t.Fatal(err)
	}
	if err = access.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	pins, err := NewPinClient(server.URL, "fixed-service-test-token", transport)
	if err != nil {
		t.Fatal(err)
	}
	if err = pins.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if transport.Jar != jar {
		t.Fatal("caller transport mutated")
	}
}
