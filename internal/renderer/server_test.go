package renderer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func gitHTTP(t *testing.T, f *gitFixture, auth string) *httptest.Server {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	backend := &cgi.Handler{Path: gitPath, Args: []string{"http-backend"}, Env: []string{
		"GIT_PROJECT_ROOT=" + f.root, "GIT_HTTP_EXPORT_ALL=1",
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=safe.directory", "GIT_CONFIG_VALUE_0=*",
	}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth != "" && r.Header.Get("Authorization") != auth {
			w.Header().Set("WWW-Authenticate", "Basic realm=\"git\"")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server
}

func testHandler(t *testing.T, cfg Config) (*Server, *Cache) {
	t.Helper()
	cache := makeCache(t, cfg)
	return NewServer(cfg, cache, slog.New(slog.NewTextHandler(io.Discard, nil))), cache
}

func renderRequest(handler http.Handler, body, auth string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/api/render", strings.NewReader(body))
	if auth != "" {
		request.Header.Set("Authorization", auth)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestServerRejectsInvalidRequests(t *testing.T) {
	cfg := testConfig(t)
	handler, _ := testHandler(t, cfg)
	if got := renderRequest(handler, "{}", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("unauthorized: %d", got)
	}
	for _, body := range []string{
		"{}",
		"{\"repo_url\":\"file:///tmp/repo\"}",
		"{\"repo_url\":\"https://github.com/a/b\",\"data\":[]}",
		"{\"repo_url\":\"https://github.com/a/b\",\"data\":null}",
		"{\"repo_url\":\"https://github.com/a/b\",\"unknown\":true}",
		"{\"repo_url\":\"https://github.com/a/b\"} {}",
		"{\"repo_url\":",
	} {
		if got := renderRequest(handler, body, "Bearer "+cfg.APIKey); got.Code != 400 {
			t.Fatalf("body=%s status=%d response=%s", body, got.Code, got.Body.String())
		}
	}
	cfg.MaxRequestBytes = 20
	handler, _ = testHandler(t, cfg)
	if got := renderRequest(handler, "{\"repo_url\":\"https://github.com/a/b\"}", "Bearer "+cfg.APIKey).Code; got != 413 {
		t.Fatalf("oversized request: %d", got)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/render", nil)
	request.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != 405 {
		t.Fatalf("wrong method: %d", recorder.Code)
	}
}

func TestServerRealGitToPDFPrivateGiteaAndRefresh(t *testing.T) {
	cfg := typstConfig(t)
	f := fixture(t)
	f.commit(t, "main.typ", "#let data = json(sys.inputs.data_file)\n= #data.title")
	gitRun(t, f.source, "push", "origin", "main")
	cfg.GiteaUsername, cfg.GiteaToken = "reader", "test-private-token"
	cfg.GiteaPublicURL, cfg.GiteaInternalURL = "http://placeholder.invalid", "http://placeholder.invalid"
	authRepo, err := cfg.Repository("http://placeholder.invalid/remote.git")
	if err != nil {
		t.Fatal(err)
	}
	remote := gitHTTP(t, f, authRepo.Auth)
	cfg.GiteaPublicURL, cfg.GiteaInternalURL = remote.URL, remote.URL
	handler, _ := testHandler(t, cfg)
	body := "{\"repo_url\":\"" + remote.URL + "/remote.git\",\"data\":{\"title\":\"Первый\"}}"
	response := renderRequest(handler, body, "Bearer "+cfg.APIKey)
	if response.Code != 200 || response.Header().Get("Content-Type") != "application/pdf" || !bytes.HasPrefix(response.Body.Bytes(), []byte("%PDF-")) || response.Header().Get("X-Source-Commit") != f.sha {
		t.Fatalf("first render: status=%d body=%s", response.Code, response.Body.String())
	}
	before := f.sha
	f.commit(t, "main.typ", "#let data = json(sys.inputs.data_file)\n= Новый документ: #data.title")
	gitRun(t, f.source, "push", "origin", "main")
	response = renderRequest(handler, body, "Bearer "+cfg.APIKey)
	if response.Code != 200 || response.Header().Get("X-Source-Commit") == before || response.Header().Get("X-Source-Commit") != f.sha {
		t.Fatalf("stale render: %d %s", response.Code, response.Body.String())
	}
	entries, _ := os.ReadDir(cfg.WorkDir)
	if len(entries) != 0 {
		t.Fatal("HTTP response leaked request files")
	}
	cfg.GiteaUsername, cfg.GiteaToken = "", ""
	noCredentials, _ := testHandler(t, cfg)
	response = renderRequest(noCredentials, body, "Bearer "+cfg.APIKey)
	if response.Code != 502 {
		t.Fatalf("private repo accessible without credentials: %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "test-private-token") {
		t.Fatal("credentials leaked in response")
	}
}

func TestServerParallelIsolationAndBusy(t *testing.T) {
	cfg := typstConfig(t)
	f := fixture(t)
	f.commit(t, "main.typ", "#let data = json(sys.inputs.data_file)\n#assert(data.title == data.expected)\n= #data.title")
	gitRun(t, f.source, "push", "origin", "main")
	remote := gitHTTP(t, f, "")
	cfg.GiteaPublicURL, cfg.GiteaInternalURL = remote.URL, remote.URL
	handler, _ := testHandler(t, cfg)
	var wg sync.WaitGroup
	for _, title := range []string{"Alice", "Bob"} {
		wg.Add(1)
		go func(title string) {
			defer wg.Done()
			body, _ := json.Marshal(map[string]any{"repo_url": remote.URL + "/remote.git", "data": map[string]string{"title": title, "expected": title}})
			res := renderRequest(handler, string(body), "Bearer "+cfg.APIKey)
			if res.Code != 200 {
				t.Errorf("parallel %s: %d %s", title, res.Code, res.Body.String())
			}
		}(title)
	}
	wg.Wait()
	handler.slots <- struct{}{}
	handler.slots <- struct{}{}
	body := "{\"repo_url\":\"" + remote.URL + "/remote.git\"}"
	if got := renderRequest(handler, body, "Bearer "+cfg.APIKey).Code; got != 429 {
		t.Fatalf("busy: %d", got)
	}
	<-handler.slots
	<-handler.slots
}

func TestServerCancellationAndDiagnosticsRedaction(t *testing.T) {
	cfg := testConfig(t)
	cfg.GiteaToken = "secret-token"
	handler, _ := testHandler(t, cfg)
	e := &APIError{Status: 422, Code: "typst_error", Message: "Compilation failed.", Diagnostics: cfg.APIKey + " " + cfg.GiteaToken + " " + filepath.Join(cfg.WorkDir, "request-abc", "project", "main.typ")}
	clean := handler.sanitize(e, cfg.WorkDir)
	if strings.Contains(clean.Diagnostics, cfg.APIKey) || strings.Contains(clean.Diagnostics, cfg.GiteaToken) || strings.Contains(clean.Diagnostics, cfg.WorkDir) {
		t.Fatal("diagnostics leaked credentials or work paths")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodPost, "/api/render", strings.NewReader("{\"repo_url\":\"https://github.com/a/b\"}")).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 408 {
		t.Fatalf("canceled: %d %s", response.Code, response.Body.String())
	}
}

func TestServerTimeoutReturnsJSONOverRealSocket(t *testing.T) {
	cfg := testConfig(t)
	cfg.RequestTimeout = time.Millisecond
	cfg.GiteaPublicURL, cfg.GiteaInternalURL = "http://gitea.invalid:3000", "http://gitea.invalid:3000"
	handler, _ := testHandler(t, cfg)
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &http.Client{Timeout: 10 * time.Second}
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/render", strings.NewReader("{\"repo_url\":\"http://gitea.invalid:3000/a/b.git\"}"))
	request.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("deadline closed connection instead of returning timeout JSON: %v", err)
	}
	defer response.Body.Close()
	var body APIError
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 504 || body.Code != "timeout" {
		t.Fatalf("timeout response: %d %+v", response.StatusCode, body)
	}
}

type logWriterFunc func([]byte) (int, error)

func (f logWriterFunc) Write(p []byte) (int, error) { return f(p) }

func TestServerFlushesErrorBeforeSlowCheckoutCleanup(t *testing.T) {
	cfg := typstConfig(t)
	f := fixture(t)
	f.commit(t, "main.typ", "#let broken = (")
	gitRun(t, f.source, "push", "origin", "main")
	remote := gitHTTP(t, f, "")
	cfg.GiteaPublicURL, cfg.GiteaInternalURL = remote.URL, remote.URL
	cache := makeCache(t, cfg)
	writer := logWriterFunc(func(p []byte) (int, error) {
		if bytes.Contains(p, []byte("project prepared")) {
			entry := cache.snapshotEntries()[0]
			if err := entry.lock(context.Background()); err != nil {
				t.Error(err)
			}
			// Simulate a second request fetching the same repository while this
			// request needs to remove its checkout after a compiler failure.
			go func() { time.Sleep(6 * time.Second); entry.unlock() }()
		}
		return len(p), nil
	})
	server := httptest.NewServer(NewServer(cfg, cache, slog.New(slog.NewTextHandler(writer, nil))))
	defer server.Close()
	client := &http.Client{Timeout: 20 * time.Second}
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/render", strings.NewReader("{\"repo_url\":\""+remote.URL+"/remote.git\"}"))
	request.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("error response was retained behind cleanup: %v", err)
	}
	defer response.Body.Close()
	content, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("error response framing was interrupted: %v", err)
	}
	var body APIError
	if err := json.Unmarshal(content, &body); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 422 || body.Code != "typst_error" {
		t.Fatalf("compiler error response: %d %s", response.StatusCode, content)
	}
}
