package renderer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var fullSHA = regexp.MustCompile(`^(?:[a-fA-F0-9]{40}|[a-fA-F0-9]{64})$`)
var cacheName = regexp.MustCompile(`^[a-f0-9]{64}\.git$`)

type cacheEntry struct {
	gate     chan struct{}
	path     string
	active   int
	pending  int // protected by Cache.mu; counts requests borrowing this gate
	lastUsed time.Time
	lastGC   time.Time
}

func (e *cacheEntry) lock(ctx context.Context) error {
	select {
	case e.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (e *cacheEntry) unlock() { <-e.gate }

type Cache struct {
	cfg     Config
	mu      sync.Mutex
	entries map[string]*cacheEntry
}

type Checkout struct {
	ProjectDir, RequestDir, Commit string
	cache                          *Cache
	entry                          *cacheEntry
	cleaned                        atomic.Bool
	cleanupMu                      sync.Mutex
}

func NewCache(cfg Config) (*Cache, error) {
	var err error
	if cfg.CacheDir, err = filepath.Abs(cfg.CacheDir); err != nil {
		return nil, err
	}
	if cfg.WorkDir, err = filepath.Abs(cfg.WorkDir); err != nil {
		return nil, err
	}
	for _, dir := range []string{cfg.CacheDir, cfg.WorkDir} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
		info, err := os.Lstat(dir)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("cache and work roots must be real directories")
		}
	}
	c := &Cache{cfg: cfg, entries: make(map[string]*cacheEntry)}
	dirs, err := os.ReadDir(cfg.CacheDir)
	if err != nil {
		return nil, err
	}
	for _, dir := range dirs {
		if dir.IsDir() && cacheName.MatchString(dir.Name()) {
			info, err := dir.Info()
			if err != nil {
				return nil, err
			}
			c.entries[dir.Name()] = &cacheEntry{gate: make(chan struct{}, 1), path: filepath.Join(cfg.CacheDir, dir.Name()), lastUsed: info.ModTime(), lastGC: time.Now()}
		}
	}
	return c, nil
}

func (c *Cache) entry(repo Repository) *cacheEntry {
	key := sha256.Sum256([]byte(repo.Identity))
	name := hex.EncodeToString(key[:]) + ".git"
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.entries[name]; existing != nil {
		existing.pending++
		return existing
	}
	e := &cacheEntry{gate: make(chan struct{}, 1), path: filepath.Join(c.cfg.CacheDir, name), pending: 1, lastUsed: time.Now(), lastGC: time.Now()}
	c.entries[name] = e
	return e
}

func (c *Cache) git(ctx context.Context, e *cacheEntry, repo Repository, args ...string) (string, error) {
	env := []string{"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull, "GIT_TERMINAL_PROMPT=0", "GIT_LFS_SKIP_SMUDGE=1", "GIT_ATTR_NOSYSTEM=1"}
	if repo.Auth != "" {
		env = append(env, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=http."+repo.URL+".extraHeader", "GIT_CONFIG_VALUE_0=Authorization: "+repo.Auth)
	}
	base := []string{
		"-c", "safe.directory=" + filepath.ToSlash(e.path),
		"-c", "core.hooksPath=" + filepath.Join(c.cfg.WorkDir, ".disabled-hooks"),
		"-c", "core.autocrlf=false", "-c", "core.longpaths=true", "-c", "credential.helper=",
		"-c", "http.followRedirects=false", "-c", "protocol.file.allow=never",
		"-c", "protocol.ext.allow=never", "-c", "fetch.fsckObjects=true",
	}
	// File transport is used only by internal tests. The public HTTP contract
	// never accepts a filesystem repository, even if Git supports it.
	if !strings.Contains(repo.URL, "://") {
		base = append(base, "-c", "protocol.file.allow=always")
	}
	return runCommand(ctx, c.cfg.GitBin, append(base, args...), e.path, env)
}

func (c *Cache) Prepare(ctx context.Context, repo Repository, ref string) (*Checkout, error) {
	if ref == "" {
		ref = "HEAD"
	}
	if strings.ContainsAny(ref, "\r\n\x00:~^?*[\\") || strings.HasPrefix(ref, "-") || strings.Contains(ref, "..") {
		return nil, failure(400, "invalid_ref", "Use a branch, tag, HEAD, or a full commit SHA.")
	}
	e := c.entry(repo)
	defer func() {
		c.mu.Lock()
		e.pending--
		c.mu.Unlock()
	}()
	if err := e.lock(ctx); err != nil {
		return nil, err
	}
	defer e.unlock()
	e.lastUsed = time.Now()
	// A prior cancelled Git command has exited before releasing this gate.
	// Its transaction locks are safe to remove before the next operation.
	if err := clearGitLocks(e.path); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(e.path, 0700); err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(e.path, "HEAD")); os.IsNotExist(err) {
		if _, err := c.git(ctx, e, repo, "init", "--bare", "."); err != nil {
			return nil, gitError(err)
		}
	}
	if ref != "HEAD" && !fullSHA.MatchString(ref) {
		check := ref
		if !strings.HasPrefix(check, "refs/") {
			check = "refs/heads/" + check
		}
		if strings.HasPrefix(ref, "refs/") && !strings.HasPrefix(ref, "refs/heads/") && !strings.HasPrefix(ref, "refs/tags/") {
			return nil, failure(400, "invalid_ref", "Use refs/heads/ or refs/tags/ for qualified refs.")
		}
		if _, err := c.git(ctx, e, repo, "check-ref-format", check); err != nil {
			return nil, failure(400, "invalid_ref", "The Git ref is invalid.")
		}
	}
	// Fetch source refs atomically. A force-push replaces tracking refs, and
	// pruning covers tags as well as branches because both refspecs are explicit.
	_, err := c.git(ctx, e, repo, "fetch", "--atomic", "--force", "--prune", "--no-recurse-submodules", "--no-auto-gc", "--", repo.URL,
		"+refs/heads/*:refs/remotes/origin/*", "+refs/tags/*:refs/tags/*", "+HEAD:refs/render/default")
	if err != nil {
		return nil, gitError(err)
	}
	commit, err := c.resolve(ctx, e, repo, ref)
	if err != nil {
		return nil, err
	}
	if err := c.checkTree(ctx, e, repo, commit); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(c.cfg.WorkDir, "request-")
	if err != nil {
		return nil, err
	}
	co := &Checkout{ProjectDir: filepath.Join(dir, "project"), RequestDir: dir, Commit: commit, cache: c, entry: e}
	if _, err := c.git(ctx, e, repo, "worktree", "add", "--detach", "--", co.ProjectDir, commit); err != nil {
		os.RemoveAll(dir)
		// Interrupted worktree creation may leave admin metadata.
		c.prune(e)
		return nil, gitError(err)
	}
	if err := c.checkFiles(ctx, co.ProjectDir); err != nil {
		c.removeCheckout(e, co)
		return nil, err
	}
	e.active++
	e.lastUsed = time.Now()
	_ = os.Chtimes(e.path, e.lastUsed, e.lastUsed)
	return co, nil
}

func (c *Cache) resolve(ctx context.Context, e *cacheEntry, repo Repository, ref string) (string, error) {
	notFound := failure(400, "ref_not_found", "The requested ref is absent or the commit is not reachable from the current remote branches or tags.")
	target := ref
	switch {
	case ref == "HEAD":
		target = "refs/render/default"
	case fullSHA.MatchString(ref):
		ref = strings.ToLower(ref)
		refs, err := c.git(ctx, e, repo, "for-each-ref", "--format=%(refname)", "--contains="+ref, "refs/remotes/origin/", "refs/tags/")
		if err != nil || refs == "" {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			return "", notFound
		}
		target = ref
	case strings.HasPrefix(ref, "refs/heads/"):
		target = "refs/remotes/origin/" + strings.TrimPrefix(ref, "refs/heads/")
	case strings.HasPrefix(ref, "refs/tags/"):
		target = ref
	default:
		_, branchErr := c.git(ctx, e, repo, "show-ref", "--verify", "--", "refs/remotes/origin/"+ref)
		_, tagErr := c.git(ctx, e, repo, "show-ref", "--verify", "--", "refs/tags/"+ref)
		if branchErr == nil && tagErr == nil {
			return "", failure(400, "ambiguous_ref", "Both a branch and a tag match; use refs/heads/ or refs/tags/.")
		}
		if branchErr == nil {
			target = "refs/remotes/origin/" + ref
		} else if tagErr == nil {
			target = "refs/tags/" + ref
		} else {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			return "", notFound
		}
	}
	sha, err := c.git(ctx, e, repo, "rev-parse", "--verify", target+"^{commit}")
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", notFound
	}
	return sha, nil
}

func (c *Cache) checkTree(ctx context.Context, e *cacheEntry, repo Repository, sha string) error {
	tree, err := c.git(ctx, e, repo, "ls-tree", "-r", "-z", "-l", sha)
	if err != nil {
		return gitError(err)
	}
	var total int64
	for _, record := range strings.Split(tree, "\x00") {
		if record == "" {
			continue
		}
		meta, name, ok := strings.Cut(record, "\t")
		fields := strings.Fields(meta)
		if !ok || len(fields) != 4 {
			return fmt.Errorf("invalid Git tree record")
		}
		if fields[0] == "160000" || fields[0] == "120000" || strings.EqualFold(filepath.Base(name), ".gitmodules") {
			return failure(400, "unsupported_project", "Symlinks and Git submodules are not supported.")
		}
		size, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil || size < 0 {
			return fmt.Errorf("invalid Git blob size")
		}
		if size > c.cfg.MaxProjectBytes-total {
			return failure(413, "project_too_large", "The checked-out project exceeds MAX_PROJECT_BYTES.")
		}
		total += size
	}
	return nil
}

func (c *Cache) checkFiles(ctx context.Context, root string) error {
	var total int64
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if entry.IsDir() || (path == filepath.Join(root, ".git")) {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return failure(400, "unsupported_project", "Symlinks are not supported.")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return failure(400, "unsupported_project", "Only ordinary files are supported.")
		}
		if info.Size() > c.cfg.MaxProjectBytes-total {
			return failure(413, "project_too_large", "The checked-out project exceeds MAX_PROJECT_BYTES.")
		}
		total += info.Size()
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		header := make([]byte, 128)
		n, readErr := file.Read(header)
		closeErr := file.Close()
		if readErr != nil && readErr != io.EOF {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if strings.HasPrefix(string(header[:n]), "version https://git-lfs.github.com/spec/v1") {
			return failure(400, "unsupported_project", "Git LFS files are not supported; commit ordinary files instead.")
		}
		return nil
	})
}

func (co *Checkout) Cleanup() error {
	co.cleanupMu.Lock()
	defer co.cleanupMu.Unlock()
	if co.cleaned.Load() {
		return nil
	}
	// Wait for any bounded fetch to exit, even after the HTTP request expires.
	// Marking completion early would leak files and prevent subsequent retries.
	if err := co.entry.lock(context.Background()); err != nil {
		return err
	}
	defer co.entry.unlock()
	if err := co.cache.removeCheckout(co.entry, co); err != nil {
		return err
	}
	co.entry.active--
	co.entry.lastUsed = time.Now()
	_ = os.Chtimes(co.entry.path, co.entry.lastUsed, co.entry.lastUsed)
	co.cleaned.Store(true)
	return nil
}

func (c *Cache) removeCheckout(e *cacheEntry, co *Checkout) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, gitErr := c.git(ctx, e, Repository{}, "worktree", "remove", "--force", "--", co.ProjectDir)
	removeErr := os.RemoveAll(co.RequestDir)
	c.prune(e)
	if removeErr != nil {
		return removeErr
	}
	if gitErr != nil {
		if _, err := os.Stat(co.ProjectDir); os.IsNotExist(err) {
			return nil
		}
		return gitErr
	}
	return nil
}

func (c *Cache) prune(e *cacheEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = c.git(ctx, e, Repository{}, "worktree", "prune", "--expire=now")
}

// Both startup recovery and callers holding the repository gate own this tree.
// No Git process is running when these abandoned transaction locks are removed.
func clearGitLocks(root string) error {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil
	}
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".lock") {
			return os.Remove(path)
		}
		return nil
	})
}

// Recover is called before serving requests, with exclusive ownership of both roots.
func (c *Cache) Recover() error {
	dirs, err := os.ReadDir(c.cfg.WorkDir)
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		if strings.HasPrefix(dir.Name(), "request-") {
			if err := os.RemoveAll(filepath.Join(c.cfg.WorkDir, dir.Name())); err != nil {
				return err
			}
		}
	}
	for _, e := range c.snapshotEntries() {
		if err := clearGitLocks(e.path); err != nil {
			return err
		}
		c.prune(e)
	}
	return nil
}

func (c *Cache) snapshotEntries() []*cacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries := make([]*cacheEntry, 0, len(c.entries))
	for _, e := range c.entries {
		entries = append(entries, e)
	}
	return entries
}

func (c *Cache) EvictIdle(now time.Time) error {
	for _, e := range c.snapshotEntries() {
		select {
		case e.gate <- struct{}{}:
		default:
			continue
		}
		c.mu.Lock()
		name := filepath.Base(e.path)
		if c.entries[name] != e || e.pending != 0 {
			c.mu.Unlock()
			e.unlock()
			continue
		}
		if e.active == 0 && now.Sub(e.lastUsed) >= c.cfg.CacheTTL {
			err := os.RemoveAll(e.path)
			if err == nil {
				delete(c.entries, name)
			}
			c.mu.Unlock()
			e.unlock()
			if err != nil {
				return err
			}
			continue
		}
		c.mu.Unlock()
		// Force-pushed history must not grow forever in a frequently used cache.
		// Only maintain repositories with no leased worktree, under the same gate.
		if e.active == 0 && now.Sub(e.lastGC) >= time.Hour {
			err := c.collect(e)
			e.lastGC = now
			if err != nil {
				e.unlock()
				return err
			}
		}
		e.unlock()
	}
	return nil
}

func (c *Cache) collect(e *cacheEntry) error {
	if err := clearGitLocks(e.path); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(e.path, "HEAD")); os.IsNotExist(err) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := c.git(ctx, e, Repository{}, "reflog", "expire", "--expire=now", "--all"); err != nil {
		return err
	}
	_, err := c.git(ctx, e, Repository{}, "gc", "--prune=now")
	return err
}
