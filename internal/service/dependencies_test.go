package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPinClientGetsSnapshotWithFixedCredentialAndStrictResponse(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet || r.URL.Path != "/internal/v1/snapshots/snapshot-1" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer storage-fixed-token" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"snapshotId":"snapshot-1","scope":{"workspaceId":"ws-1","projectId":"project-1"},"manifest":{"formatVersion":1,"id":"identity","origin":{"kind":"folder","location":"source"},"files":[],"directories":[],"fonts":[]},"resources":[],"createdAt":"2026-10-15T00:00:00Z"}`)
	}))
	defer server.Close()

	client, err := NewPinClient(server.URL, "storage-fixed-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.GetSnapshot(context.Background(), "snapshot-1")
	if err != nil || snapshot.SnapshotID != "snapshot-1" || snapshot.Scope.ProjectID != "project-1" {
		t.Fatalf("snapshot = %#v, err %v", snapshot, err)
	}
	if calls != 1 {
		t.Fatalf("Storage request count = %d", calls)
	}
}

func TestPinClientDoesNotFollowStorageRedirect(t *testing.T) {
	var redirected int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected++ }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/internal/v1/snapshots/snapshot-1", http.StatusFound)
	}))
	defer source.Close()

	client, err := NewPinClient(source.URL, "storage-fixed-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.GetSnapshot(context.Background(), "snapshot-1"); !isHTTPStatus(err, http.StatusFound) {
		t.Fatalf("redirect response error = %v", err)
	}
	if redirected != 0 {
		t.Fatalf("Storage redirect was followed %d times", redirected)
	}
}

func TestDependencyReadinessFailsClosed(t *testing.T) {
	status := http.StatusServiceUnavailable
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		if r.URL.Path == "/readyz" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"allowed":false}`)
	}))
	defer server.Close()
	pins, _ := NewPinClient(server.URL, "fixed", nil)
	access, _ := NewAccessClient(server.URL, "fixed", nil)
	if pins.Ready(context.Background()) == nil || access.Ready(context.Background()) == nil {
		t.Fatal("offline dependency is ready")
	}
	status = http.StatusOK
	if err := pins.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := access.Ready(context.Background()); err != nil {
		t.Fatal("authoritative denial is healthy:", err)
	}
}
