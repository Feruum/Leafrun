package renderer

import (
	"net/http"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	t.Setenv("API_KEY", "test-key")
	for _, key := range []string{"REQUEST_TIMEOUT", "MAX_CONCURRENT", "MAX_REQUEST_BYTES", "MAX_PROJECT_BYTES", "CACHE_TTL", "GITEA_PUBLIC_URL", "GITEA_INTERNAL_URL", "GITEA_USERNAME", "GITEA_TOKEN"} {
		t.Setenv(key, "")
	}
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RequestTimeout != time.Minute || cfg.MaxConcurrent != 2 || cfg.MaxRequestBytes != 1<<20 || cfg.MaxProjectBytes != 100<<20 || cfg.CacheTTL != 24*time.Hour {
		t.Fatalf("unexpected defaults: timeout=%v concurrent=%v request=%v project=%v ttl=%v", cfg.RequestTimeout, cfg.MaxConcurrent, cfg.MaxRequestBytes, cfg.MaxProjectBytes, cfg.CacheTTL)
	}
}

func TestConfigRejectsMissingKeyAndInvalidLimits(t *testing.T) {
	t.Setenv("API_KEY", "")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("missing API key was accepted")
	}
	t.Setenv("API_KEY", "test-key")
	for _, key := range []string{"MAX_CONCURRENT", "MAX_REQUEST_BYTES", "MAX_PROJECT_BYTES", "REQUEST_TIMEOUT", "CACHE_TTL"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "-1")
			if _, err := LoadConfig(); err == nil {
				t.Fatalf("invalid %s was accepted", key)
			}
		})
	}
}

func TestRepositoryPolicy(t *testing.T) {
	cfg := testConfig(t)
	cfg.AllowedGitHosts = []string{"github.com", "gitlab.com"}
	cfg.GiteaPublicURL = "http://localhost:3000/git/"
	cfg.GiteaInternalURL = "http://gitea:3000/"
	cfg.GiteaUsername = "reader"
	cfg.GiteaToken = "private-token"
	repo, err := cfg.Repository("http://localhost:3000/git/alice/report.git")
	if err != nil || repo.URL != "http://gitea:3000/alice/report.git" || repo.Auth == "" {
		t.Fatalf("Gitea mapping: repo=%+v err=%v", repo, err)
	}
	public, err := cfg.Repository("https://github.com/alice/report.git")
	if err != nil || public.Auth != "" {
		t.Fatalf("public repo inherited credentials: %+v, %v", public, err)
	}
	for _, raw := range []string{
		"file:///etc/passwd", "git@github.com:alice/report.git",
		"http://github.com/alice/report.git", "https://evil.test/report.git",
		"https://user:password@github.com/alice/report.git",
		"https://github.com/alice/report.git?token=secret",
		"http://localhost:3000/other/report.git", "http://gitea:3000/alice/../report.git",
		"https://github.com/alice/%2e%2e/report.git",
	} {
		if _, err := cfg.Repository(raw); err == nil {
			t.Errorf("accepted unsafe URL %q", raw)
		}
	}
}

func testConfig(t *testing.T) Config {
	t.Helper()
	root := t.TempDir()
	return Config{
		Addr: ":8080", APIKey: "test-key", GitBin: "git", TypstBin: "typst",
		CacheDir: root + "/cache", WorkDir: root + "/work",
		RequestTimeout: time.Minute, MaxConcurrent: 2,
		MaxRequestBytes: 1 << 20, MaxProjectBytes: 100 << 20,
		CacheTTL: 24 * time.Hour, AllowedGitHosts: []string{"github.com", "gitlab.com"},
	}
}

func requireErrorCode(t *testing.T, err error, code string, status int) {
	t.Helper()
	e := asAPIError(err)
	if err == nil || e.Code != code || e.Status != status {
		t.Fatalf("wanted %s (%d), got %#v, %v", code, status, e, err)
	}
}

func TestInvalidRepositoryIsClientError(t *testing.T) {
	_, err := testConfig(t).Repository("not a URL")
	requireErrorCode(t, err, "invalid_repository", http.StatusBadRequest)
}
