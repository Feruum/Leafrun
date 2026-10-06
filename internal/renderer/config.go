package renderer

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr, APIKey, GitBin, TypstBin, CacheDir, WorkDir string
	RequestTimeout, CacheTTL                          time.Duration
	MaxConcurrent                                     int
	MaxRequestBytes, MaxProjectBytes                  int64
	AllowedGitHosts                                   []string
	GiteaPublicURL, GiteaInternalURL                  string
	GiteaUsername, GiteaToken                         string
}

type Repository struct {
	URL      string
	Identity string
	Auth     string
}

func LoadConfig() (Config, error) {
	cfg := Config{
		Addr: envOr("HTTP_ADDR", ":8080"), APIKey: os.Getenv("API_KEY"),
		GitBin: envOr("GIT_BIN", "git"), TypstBin: envOr("TYPST_BIN", "typst"),
		CacheDir:        envOr("CACHE_DIR", filepath.Join(os.TempDir(), "typst-render", "cache")),
		WorkDir:         envOr("WORK_DIR", filepath.Join(os.TempDir(), "typst-render", "work")),
		AllowedGitHosts: strings.Split(envOr("GIT_ALLOWED_HOSTS", "github.com,gitlab.com"), ","),
		GiteaPublicURL:  os.Getenv("GITEA_PUBLIC_URL"), GiteaInternalURL: os.Getenv("GITEA_INTERNAL_URL"),
		GiteaUsername: os.Getenv("GITEA_USERNAME"), GiteaToken: os.Getenv("GITEA_TOKEN"),
	}
	var err error
	if cfg.RequestTimeout, err = time.ParseDuration(envOr("REQUEST_TIMEOUT", "60s")); err != nil || cfg.RequestTimeout <= 0 {
		return cfg, fmt.Errorf("REQUEST_TIMEOUT must be a positive duration")
	}
	if cfg.CacheTTL, err = time.ParseDuration(envOr("CACHE_TTL", "24h")); err != nil || cfg.CacheTTL <= 0 {
		return cfg, fmt.Errorf("CACHE_TTL must be a positive duration")
	}
	if cfg.MaxConcurrent, err = strconv.Atoi(envOr("MAX_CONCURRENT", "2")); err != nil || cfg.MaxConcurrent < 1 {
		return cfg, fmt.Errorf("MAX_CONCURRENT must be a positive integer")
	}
	for key, dest := range map[string]*int64{"MAX_REQUEST_BYTES": &cfg.MaxRequestBytes, "MAX_PROJECT_BYTES": &cfg.MaxProjectBytes} {
		defaultValue := "1048576"
		if key == "MAX_PROJECT_BYTES" {
			defaultValue = "104857600"
		}
		if *dest, err = strconv.ParseInt(envOr(key, defaultValue), 10, 64); err != nil || *dest < 1 {
			return cfg, fmt.Errorf("%s must be a positive integer", key)
		}
	}
	return cfg, cfg.Validate()
}

func (cfg Config) Validate() error {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return fmt.Errorf("API_KEY is required")
	}
	if cfg.RequestTimeout <= 0 || cfg.CacheTTL <= 0 || cfg.MaxConcurrent < 1 || cfg.MaxRequestBytes < 1 || cfg.MaxProjectBytes < 1 {
		return fmt.Errorf("request limits and cache TTL must be positive")
	}
	cache, err := filepath.Abs(cfg.CacheDir)
	if err != nil {
		return err
	}
	work, err := filepath.Abs(cfg.WorkDir)
	if err != nil {
		return err
	}
	if containsPath(cache, work) || containsPath(work, cache) {
		return fmt.Errorf("CACHE_DIR and WORK_DIR must be separate, non-nested directories")
	}
	if (cfg.GiteaPublicURL == "") != (cfg.GiteaInternalURL == "") {
		return fmt.Errorf("GITEA_PUBLIC_URL and GITEA_INTERNAL_URL must be configured together")
	}
	for _, raw := range []string{cfg.GiteaPublicURL, cfg.GiteaInternalURL} {
		if raw != "" {
			if _, err := parseGitURL(raw); err != nil {
				return fmt.Errorf("invalid Gitea base URL")
			}
		}
	}
	if (cfg.GiteaUsername == "") != (cfg.GiteaToken == "") || (cfg.GiteaToken != "" && cfg.GiteaPublicURL == "") {
		return fmt.Errorf("Gitea credentials require both username and token and configured Gitea URLs")
	}
	if strings.ContainsAny(cfg.GiteaUsername+cfg.GiteaToken, "\r\n\x00") || strings.Contains(cfg.GiteaUsername, ":") {
		return fmt.Errorf("invalid Gitea credentials")
	}
	return nil
}

func (cfg Config) Repository(raw string) (Repository, error) {
	bad := failure(http.StatusBadRequest, "invalid_repository", "Use a Git repository URL on a configured host, without credentials or query parameters.")
	u, err := parseGitURL(raw)
	if err != nil || u.Path == "" || u.Path == "/" {
		return Repository{}, bad
	}
	for _, base := range []string{cfg.GiteaPublicURL, cfg.GiteaInternalURL} {
		if base == "" {
			continue
		}
		b, _ := url.Parse(base)
		prefix := strings.TrimRight(b.Path, "/") + "/"
		if u.Scheme == b.Scheme && strings.EqualFold(u.Host, b.Host) && strings.HasPrefix(u.Path, prefix) {
			rel := strings.TrimPrefix(u.Path, prefix)
			if rel == "" {
				return Repository{}, bad
			}
			internal, _ := url.Parse(cfg.GiteaInternalURL)
			internal.Path = strings.TrimRight(internal.Path, "/") + "/" + rel
			internal.RawPath = ""
			repo := Repository{URL: internal.String(), Identity: internal.String()}
			if cfg.GiteaToken != "" {
				repo.Auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(cfg.GiteaUsername+":"+cfg.GiteaToken))
			}
			return repo, nil
		}
	}
	if u.Scheme != "https" {
		return Repository{}, bad
	}
	for _, host := range cfg.AllowedGitHosts {
		if strings.EqualFold(u.Host, strings.TrimSpace(host)) {
			return Repository{URL: u.String(), Identity: u.String()}, nil
		}
	}
	return Repository{}, bad
}

func parseGitURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(raw, "\r\n\x00\\") {
		return nil, fmt.Errorf("invalid Git URL")
	}
	for _, segment := range strings.Split(u.Path, "/") {
		if segment == "." || segment == ".." {
			return nil, fmt.Errorf("invalid Git URL path")
		}
	}
	u.Host = strings.ToLower(u.Host)
	return u, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func containsPath(root, target string) bool {
	rel, err := filepath.Rel(root, target)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}
