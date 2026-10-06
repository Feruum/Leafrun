package renderer

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type gitFixture struct{ root, source, remote, sha string }

func gitRun(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "safe.directory=*", "-c", "core.autocrlf=false"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func fixture(t *testing.T) *gitFixture {
	t.Helper()
	f := &gitFixture{root: t.TempDir()}
	f.source, f.remote = filepath.Join(f.root, "source"), filepath.Join(f.root, "remote.git")
	if err := os.MkdirAll(f.source, 0700); err != nil {
		t.Fatal(err)
	}
	gitRun(t, f.source, "init", "-b", "main")
	gitRun(t, f.source, "config", "user.name", "Test")
	gitRun(t, f.source, "config", "user.email", "test@example.invalid")
	f.commit(t, "main.typ", "= First")
	gitRun(t, f.root, "clone", "--bare", f.source, f.remote)
	gitRun(t, f.source, "remote", "add", "origin", f.remote)
	return f
}

func (f *gitFixture) commit(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(f.source, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, f.source, "add", "--", name)
	gitRun(t, f.source, "commit", "-m", "update fixture")
	f.sha = gitRun(t, f.source, "rev-parse", "HEAD")
	return f.sha
}

func (f *gitFixture) repo() Repository { return Repository{URL: f.remote, Identity: f.remote} }

func makeCache(t *testing.T, cfg Config) *Cache {
	t.Helper()
	c, err := NewCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func prepare(t *testing.T, c *Cache, repo Repository, ref string) *Checkout {
	t.Helper()
	co, err := c.Prepare(context.Background(), repo, ref)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := co.Cleanup(); err != nil {
			t.Errorf("checkout cleanup: %v", err)
		}
	})
	return co
}

func TestCacheFreshnessAndPinnedSnapshots(t *testing.T) {
	f := fixture(t)
	c := makeCache(t, testConfig(t))
	old := prepare(t, c, f.repo(), "HEAD")
	if old.Commit != f.sha {
		t.Fatalf("wrong commit: %s", old.Commit)
	}
	original := f.sha
	next := f.commit(t, "main.typ", "= Updated")
	gitRun(t, f.source, "push", "origin", "main")
	fresh := prepare(t, c, f.repo(), "main")
	oldContent, _ := os.ReadFile(filepath.Join(old.ProjectDir, "main.typ"))
	if fresh.Commit != next || string(oldContent) != "= First" || old.ProjectDir == fresh.ProjectDir {
		t.Fatal("snapshots were stale or shared")
	}
	pinned := prepare(t, c, f.repo(), original)
	if pinned.Commit != original {
		t.Fatal("reachable historical SHA did not resolve")
	}
	entries, err := os.ReadDir(c.cfg.CacheDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("repository cache was not reused: %v %v", entries, err)
	}
}

func TestCacheForcePushAndReachability(t *testing.T) {
	f := fixture(t)
	c := makeCache(t, testConfig(t))
	first := f.sha
	removed := f.commit(t, "main.typ", "= Temporary")
	gitRun(t, f.source, "push", "origin", "main")
	prepare(t, c, f.repo(), removed)
	gitRun(t, f.source, "reset", "--hard", first)
	gitRun(t, f.source, "push", "--force", "origin", "main")
	if got := prepare(t, c, f.repo(), "main"); got.Commit != first {
		t.Fatal("force-pushed branch remained stale")
	}
	_, err := c.Prepare(context.Background(), f.repo(), removed)
	requireErrorCode(t, err, "ref_not_found", http.StatusBadRequest)
}

func TestCacheDeletedRefsAmbiguityAndDefaultBranch(t *testing.T) {
	f := fixture(t)
	c := makeCache(t, testConfig(t))
	gitRun(t, f.source, "branch", "dev")
	gitRun(t, f.source, "tag", "dev")
	gitRun(t, f.source, "push", "origin", "refs/heads/dev", "refs/tags/dev")
	_, err := c.Prepare(context.Background(), f.repo(), "dev")
	requireErrorCode(t, err, "ambiguous_ref", http.StatusBadRequest)
	prepare(t, c, f.repo(), "refs/heads/dev")
	prepare(t, c, f.repo(), "refs/tags/dev")
	gitRun(t, f.source, "push", "origin", ":refs/heads/dev", ":refs/tags/dev")
	for _, ref := range []string{"refs/heads/dev", "refs/tags/dev"} {
		_, err = c.Prepare(context.Background(), f.repo(), ref)
		requireErrorCode(t, err, "ref_not_found", http.StatusBadRequest)
	}
	gitRun(t, f.source, "checkout", "-b", "new-default")
	sha := f.commit(t, "main.typ", "= New default")
	gitRun(t, f.source, "push", "origin", "new-default")
	gitRun(t, f.remote, "symbolic-ref", "HEAD", "refs/heads/new-default")
	if got := prepare(t, c, f.repo(), "HEAD"); got.Commit != sha {
		t.Fatal("remote default branch was not refreshed")
	}
}

func TestCacheFetchFailureNeverRendersStale(t *testing.T) {
	f := fixture(t)
	c := makeCache(t, testConfig(t))
	prepare(t, c, f.repo(), "HEAD")
	if err := os.Rename(f.remote, f.remote+".offline"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Prepare(context.Background(), f.repo(), "HEAD")
	requireErrorCode(t, err, "git_error", http.StatusBadGateway)
}

func TestCacheCleanupRecoveryAndExpiration(t *testing.T) {
	f := fixture(t)
	cfg := testConfig(t)
	c := makeCache(t, cfg)
	co := prepare(t, c, f.repo(), "HEAD")
	if err := c.EvictIdle(time.Now().Add(48 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(co.ProjectDir); err != nil {
		t.Fatal("active checkout evicted")
	}
	if err := co.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(co.RequestDir); !os.IsNotExist(err) {
		t.Fatal("request directory survived cleanup")
	}
	orphan := prepare(t, c, f.repo(), "HEAD")
	restarted := makeCache(t, cfg)
	if err := restarted.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan.RequestDir); !os.IsNotExist(err) {
		t.Fatal("startup retained abandoned checkout")
	}
	// The old owner is no longer running; do not run its lease cleanup against
	// a cache that startup recovery intentionally replaced.
	orphan.cleaned.Store(true)
	next := prepare(t, restarted, f.repo(), "HEAD")
	next.Cleanup()
	if err := restarted.EvictIdle(time.Now().Add(48 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(cfg.CacheDir)
	if len(entries) != 0 {
		t.Fatal("idle repository cache was retained")
	}
	if len(restarted.entries) != 0 {
		t.Fatal("evicted repository identities were retained in memory")
	}
	prepare(t, restarted, f.repo(), "HEAD")
}

func TestCacheCollectsUnreachableObjectsAfterForcePush(t *testing.T) {
	f := fixture(t)
	c := makeCache(t, testConfig(t))
	first := f.sha
	removed := f.commit(t, "main.typ", "= Abandoned revision")
	gitRun(t, f.source, "push", "origin", "main")
	old := prepare(t, c, f.repo(), "HEAD")
	old.Cleanup()
	gitRun(t, f.source, "reset", "--hard", first)
	gitRun(t, f.source, "push", "--force", "origin", "main")
	fresh := prepare(t, c, f.repo(), "HEAD")
	fresh.Cleanup()
	if err := c.EvictIdle(time.Now().Add(2 * time.Hour)); err != nil {
		if detail, ok := err.(*commandError); ok {
			t.Fatalf("maintenance: %v: %s", err, detail.diagnostics)
		}
		t.Fatal(err)
	}
	if _, err := c.git(context.Background(), fresh.entry, f.repo(), "cat-file", "-e", removed); err == nil {
		t.Fatal("unreachable Git objects survived idle maintenance")
	}
	if got := prepare(t, c, f.repo(), first); got.Commit != first {
		t.Fatal("maintenance removed reachable history")
	}
}

func TestCacheRejectsUnsupportedAndOversizedProjects(t *testing.T) {
	for _, tc := range []struct {
		name, content, code string
		limit               int64
	}{
		{".gitmodules", "[submodule \"x\"]\npath = x\nurl = https://example.invalid/x", "unsupported_project", 100 << 20},
		{"image.png", "version https://git-lfs.github.com/spec/v1\noid sha256:abcd\nsize 20\n", "unsupported_project", 100 << 20},
		{"large.txt", strings.Repeat("x", 200), "project_too_large", 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture(t)
			f.commit(t, tc.name, tc.content)
			gitRun(t, f.source, "push", "origin", "main")
			cfg := testConfig(t)
			cfg.MaxProjectBytes = tc.limit
			c := makeCache(t, cfg)
			_, err := c.Prepare(context.Background(), f.repo(), "HEAD")
			status := http.StatusBadRequest
			if tc.code == "project_too_large" {
				status = http.StatusRequestEntityTooLarge
			}
			requireErrorCode(t, err, tc.code, status)
			entries, _ := os.ReadDir(cfg.WorkDir)
			if len(entries) != 0 {
				t.Fatal("rejected checkout leaked request directory")
			}
		})
	}
}

func TestCacheRefArgumentsCannotBecomeOptions(t *testing.T) {
	f := fixture(t)
	c := makeCache(t, testConfig(t))
	for _, ref := range []string{"--upload-pack=bad", "main:refs/heads/evil", "main\nrefs/tags/x", "HEAD~1"} {
		_, err := c.Prepare(context.Background(), f.repo(), ref)
		requireErrorCode(t, err, "invalid_ref", http.StatusBadRequest)
	}
}

func TestCacheCleanupWaitsForConcurrentCleanup(t *testing.T) {
	f := fixture(t)
	c := makeCache(t, testConfig(t))
	co := prepare(t, c, f.repo(), "HEAD")
	co.entry.gate <- struct{}{}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- co.Cleanup() }()
	time.Sleep(20 * time.Millisecond)
	go func() { second <- co.Cleanup() }()
	early := false
	select {
	case <-second:
		early = true
	case <-time.After(50 * time.Millisecond):
	}
	co.entry.unlock()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if !early {
		if err := <-second; err != nil {
			t.Fatal(err)
		}
	}
	if early {
		t.Fatal("Cleanup returned while worktree, JSON and PDF were still retained")
	}
	if co.entry.active != 0 {
		t.Fatal("cleanup did not release exactly one lease")
	}
}

func TestCacheMeasuresExpandedCheckout(t *testing.T) {
	f := fixture(t)
	f.commit(t, ".gitattributes", "*.txt ident\n")
	f.commit(t, "expanded.txt", strings.Repeat("$Id$\n", 20))
	gitRun(t, f.source, "push", "origin", "main")
	cfg := testConfig(t)
	cfg.MaxProjectBytes = 200
	c := makeCache(t, cfg)
	_, err := c.Prepare(context.Background(), f.repo(), "HEAD")
	requireErrorCode(t, err, "project_too_large", 413)
}

func TestCacheRecoversAbandonedGitTransaction(t *testing.T) {
	f := fixture(t)
	cfg := testConfig(t)
	c := makeCache(t, cfg)
	co := prepare(t, c, f.repo(), "HEAD")
	co.Cleanup()
	lock := filepath.Join(co.entry.path, "refs", "remotes", "origin", "main.lock")
	if err := os.WriteFile(lock, []byte("interrupted fetch"), 0600); err != nil {
		t.Fatal(err)
	}
	f.commit(t, "main.typ", "= After interrupted fetch")
	gitRun(t, f.source, "push", "origin", "main")
	restarted := makeCache(t, cfg)
	if err := restarted.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := prepare(t, restarted, f.repo(), "HEAD"); got.Commit != f.sha {
		t.Fatal("orphan Git lock blocked freshness recovery")
	}
}

func TestCacheConcurrentVersionSnapshots(t *testing.T) {
	f := fixture(t)
	first := f.sha
	latest := f.commit(t, "main.typ", "= Concurrent revision")
	gitRun(t, f.source, "push", "origin", "main")
	c := makeCache(t, testConfig(t))
	type result struct {
		checkout *Checkout
		err      error
	}
	results := make(chan result, 4)
	for _, ref := range []string{first, "HEAD", first, "main"} {
		go func(ref string) {
			co, err := c.Prepare(context.Background(), f.repo(), ref)
			results <- result{co, err}
		}(ref)
	}
	seen := make(map[string]bool)
	for range 4 {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		co := got.checkout
		t.Cleanup(func() {
			if err := co.Cleanup(); err != nil {
				t.Error(err)
			}
		})
		content, err := os.ReadFile(filepath.Join(co.ProjectDir, "main.typ"))
		if err != nil {
			t.Fatal(err)
		}
		if seen[co.ProjectDir] {
			t.Fatal("concurrent requests shared their working directory")
		}
		seen[co.ProjectDir] = true
		want := "= First"
		if co.Commit == latest {
			want = "= Concurrent revision"
		} else if co.Commit != first {
			t.Fatal("unexpected snapshot commit")
		}
		if string(content) != want {
			t.Fatal("concurrent versions mixed source files")
		}
	}
	if err := c.EvictIdle(time.Now().Add(48 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	for path := range seen {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("active concurrent snapshot was evicted")
		}
	}
}

func TestCacheRejectsGitSymlinkBeforeCheckout(t *testing.T) {
	f := fixture(t)
	blob := gitRun(t, f.source, "rev-parse", "HEAD:main.typ")
	gitRun(t, f.source, "update-index", "--add", "--cacheinfo", "120000", blob, "link.typ")
	gitRun(t, f.source, "commit", "-m", "add symlink")
	gitRun(t, f.source, "push", "origin", "main")
	c := makeCache(t, testConfig(t))
	_, err := c.Prepare(context.Background(), f.repo(), "HEAD")
	requireErrorCode(t, err, "unsupported_project", 400)
	dirs, _ := os.ReadDir(c.cfg.WorkDir)
	if len(dirs) != 0 {
		t.Fatal("symlink project reached checkout")
	}
}
