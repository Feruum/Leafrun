// Package bundle imports verified renderwire snapshot bundles into private,
// opaque disk staging. Logical paths are never used as native filenames.
package bundle

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"sync"
	"time"

	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

const (
	stagePrefix  = "leafrun-bundle-"
	manifestFile = "manifest.json"
	markerFile   = "verified.marker"
	payloadDir   = "payload"
	filePrefix   = "f"
)

var tokenPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

// Options selects the trusted parent directory for the private stage. Each
// bundle gets a new 0700 owned directory directly beneath it.
type Options struct {
	TempDir string
}

// StagedBundle owns one verified bundle copy. Its native root and opaque
// payload names are private implementation state and never enter a manifest.
type StagedBundle struct {
	mu           sync.RWMutex
	root         string
	token        string
	manifest     storagev1.SnapshotManifest
	byDescriptor map[renderwire.FileDescriptor]int
	project      *logicalTree
	fonts        map[int]*logicalTree
	open         map[*verifiedFile]struct{}
	closed       bool
	closeErr     error
}

// Stage reads exactly the next manifest and its ordered files. It returns only
// after ReadBundle has consumed and verified every declared file frame. The
// following input terminal and EOF belong to the caller's protocol decoder.
func Stage(ctx context.Context, decoder *renderwire.Decoder, options Options) (_ *StagedBundle, err error) {
	if decoder == nil {
		return nil, errors.New("bundle decoder is required")
	}
	if ctx == nil {
		return nil, errors.New("bundle staging context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parent, err := privateParent(options.TempDir)
	if err != nil {
		return nil, err
	}
	token, err := newToken()
	if err != nil {
		return nil, err
	}
	root := filepath.Join(parent, stagePrefix+token)
	if err = os.Mkdir(root, 0o700); err != nil {
		return nil, fmt.Errorf("create private bundle stage: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			err = errors.Join(err, os.RemoveAll(root))
		}
	}()
	payload := filepath.Join(root, payloadDir)
	if err = os.Mkdir(payload, 0o700); err != nil {
		return nil, fmt.Errorf("create bundle payload directory: %w", err)
	}
	var sequence int
	manifest, err := renderwire.ReadBundle(decoder, func(_ renderwire.FileDescriptor, input io.Reader) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := opaqueName(sequence)
		sequence++
		file, err := os.OpenFile(filepath.Join(payload, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("create staged payload: %w", err)
		}
		_, copyErr := copyContext(ctx, file, input)
		syncErr := file.Sync()
		closeErr := file.Close()
		if copyErr != nil || syncErr != nil || closeErr != nil {
			return errors.Join(copyErr, syncErr, closeErr)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read bundle frames: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	files, err := renderwire.PlanFiles(manifest)
	if err != nil {
		return nil, err
	}
	if len(files) != sequence {
		return nil, errors.New("bundle staging file count mismatch")
	}
	encodedManifest, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	if int64(len(encodedManifest)) > renderwire.MetadataLimit {
		return nil, errors.New("bundle manifest exceeds metadata limit")
	}
	manifestHash := sha256.Sum256(encodedManifest)
	if err := writePrivateFile(filepath.Join(root, manifestFile), encodedManifest); err != nil {
		return nil, err
	}
	// This marker is created only after every bounded frame was consumed and
	// verified by ReadBundle. It lets a trusted controller reopen a stage after
	// its process restarts without treating a partial import as complete.
	if err := writePrivateFile(filepath.Join(root, markerFile), []byte(hex.EncodeToString(manifestHash[:])+"\n")); err != nil {
		return nil, err
	}
	staged, err := buildStaged(root, token, manifest, files)
	if err != nil {
		return nil, err
	}
	keep = true
	return staged, nil
}

// Reopen loads a previously verified bundle by its opaque token. Every
// manifest, marker and payload file is checked again before a usable stage is
// returned. The token is constrained to a single direct child of tempDir.
func Reopen(ctx context.Context, tempDir, token string) (_ *StagedBundle, err error) {
	if ctx == nil {
		return nil, errors.New("bundle reopening context is required")
	}
	if !tokenPattern.MatchString(token) {
		return nil, errors.New("invalid bundle stage token")
	}
	parent, err := privateParent(tempDir)
	if err != nil {
		return nil, err
	}
	root := filepath.Join(parent, stagePrefix+token)
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect bundle stage: %w", err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 || !privatePermissions(rootInfo.Mode()) {
		return nil, errors.New("bundle stage root is not a private ordinary directory")
	}
	rootEntries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	if len(rootEntries) != 3 || rootEntries[0].Name() != manifestFile || rootEntries[1].Name() != payloadDir || rootEntries[2].Name() != markerFile {
		return nil, errors.New("bundle stage is incomplete")
	}
	manifestPath := filepath.Join(root, manifestFile)
	if err := requirePrivateRegular(manifestPath, renderwire.MetadataLimit); err != nil {
		return nil, err
	}
	manifestBytes, err := readBounded(manifestPath, renderwire.MetadataLimit)
	if err != nil {
		return nil, err
	}
	var manifest storagev1.SnapshotManifest
	if err = renderwire.DecodeJSON(manifestBytes, &manifest); err != nil {
		return nil, fmt.Errorf("decode staged manifest: %w", err)
	}
	files, err := renderwire.PlanFiles(manifest)
	if err != nil {
		return nil, fmt.Errorf("validate staged manifest: %w", err)
	}
	markerPath := filepath.Join(root, markerFile)
	if err := requirePrivateRegular(markerPath, 128); err != nil {
		return nil, err
	}
	marker, err := readBounded(markerPath, 128)
	if err != nil {
		return nil, err
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	expectedMarker := []byte(hex.EncodeToString(manifestDigest[:]) + "\n")
	if !bytesEqual(marker, expectedMarker) {
		return nil, errors.New("staged bundle marker does not match its manifest")
	}
	payload := filepath.Join(root, payloadDir)
	if err := requirePrivateDirectory(payload); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(payload)
	if err != nil {
		return nil, err
	}
	if len(entries) != len(files) {
		return nil, errors.New("staged bundle payload is incomplete")
	}
	for index, descriptor := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := opaqueName(index)
		if entries[index].Name() != name {
			return nil, errors.New("staged bundle payload index is corrupt")
		}
		if err := verifyPayload(ctx, filepath.Join(payload, name), descriptor); err != nil {
			return nil, err
		}
	}
	return buildStaged(root, token, manifest, files)
}

func (s *StagedBundle) Token() string { return s.token }

// Manifest returns a deep copy; callers cannot change the identity or staging index.
func (s *StagedBundle) Manifest() storagev1.SnapshotManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneManifest(s.manifest)
}

// ProjectFS returns a logical filesystem backed by opaque staged names.
func (s *StagedBundle) ProjectFS() fs.FS { return treeFS{stage: s, tree: s.project} }

// FontFS returns only an external font tree. In-project font trees are already
// part of ProjectFS and do not receive duplicate storage.
func (s *StagedBundle) FontFS(index int) (fs.FS, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, fs.ErrClosed
	}
	tree, ok := s.fonts[index]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return treeFS{stage: s, tree: tree}, nil
}

// Bundle is the verified manifest and its logical project/external-font trees.
func (s *StagedBundle) Bundle() (renderwire.Bundle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return renderwire.Bundle{}, fs.ErrClosed
	}
	external := make(map[int]fs.FS, len(s.fonts))
	for index, tree := range s.fonts {
		external[index] = treeFS{stage: s, tree: tree}
	}
	return renderwire.Bundle{Manifest: cloneManifest(s.manifest), ProjectFS: treeFS{stage: s, tree: s.project}, ExternalFontFS: external}, nil
}

// OpenVerified opens exactly a descriptor in this manifest. Payload bytes are
// streamed and checked for both size and SHA-256 when the file reaches EOF.
func (s *StagedBundle) OpenVerified(descriptor renderwire.FileDescriptor) (fs.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fs.ErrClosed
	}
	index, ok := s.byDescriptor[descriptor]
	if !ok {
		return nil, errors.New("file descriptor is not in this bundle")
	}
	file, err := os.Open(filepath.Join(s.root, payloadDir, opaqueName(index)))
	if err != nil {
		return nil, err
	}
	info := stagedInfo{name: path.Base(descriptor.Path), size: descriptor.Size}
	opened := &verifiedFile{stage: s, file: file, info: info, remaining: descriptor.Size}
	copy(opened.expected[:], mustDecodeHash(descriptor.SHA256))
	s.open[opened] = struct{}{}
	return opened, nil
}

func (s *StagedBundle) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	for opened := range s.open {
		opened.mu.Lock()
		if !opened.closed {
			if err := opened.file.Close(); err != nil {
				s.closeErr = errors.Join(s.closeErr, err)
			}
			opened.closed = true
		}
		opened.mu.Unlock()
		delete(s.open, opened)
	}
	if s.root != "" {
		s.closeErr = errors.Join(s.closeErr, os.RemoveAll(s.root))
	}
	return s.closeErr
}

func buildStaged(root, token string, manifest storagev1.SnapshotManifest, files []renderwire.FileDescriptor) (*StagedBundle, error) {
	project := newLogicalTree()
	fonts := make(map[int]*logicalTree)
	for index, font := range manifest.Fonts {
		if !font.InProject {
			fonts[index] = newLogicalTree()
		}
	}
	byDescriptor := make(map[renderwire.FileDescriptor]int, len(files))
	for i, descriptor := range files {
		byDescriptor[descriptor] = i
		tree := project
		if descriptor.Tree == "font" {
			tree = fonts[descriptor.FontIndex]
			if tree == nil {
				return nil, errors.New("manifest external font index is missing")
			}
		}
		tree.addFile(descriptor, i)
	}
	for _, directory := range manifest.Directories {
		project.addDirectory(directory)
	}
	for index, font := range manifest.Fonts {
		if !font.InProject {
			for _, directory := range font.Directories {
				fonts[index].addDirectory(directory)
			}
		}
	}
	for _, tree := range append([]*logicalTree{project}, fontTrees(fonts)...) {
		tree.sortChildren()
	}
	return &StagedBundle{root: root, token: token, manifest: cloneManifest(manifest), byDescriptor: byDescriptor, project: project, fonts: fonts, open: make(map[*verifiedFile]struct{})}, nil
}

func fontTrees(fonts map[int]*logicalTree) []*logicalTree {
	indices := make([]int, 0, len(fonts))
	for index := range fonts {
		indices = append(indices, index)
	}
	sort.Ints(indices)
	out := make([]*logicalTree, 0, len(indices))
	for _, index := range indices {
		out = append(out, fonts[index])
	}
	return out
}

func (s *StagedBundle) openDescriptor(descriptor renderwire.FileDescriptor) (fs.File, error) {
	return s.OpenVerified(descriptor)
}

type logicalTree struct {
	files    map[string]renderwire.FileDescriptor
	fileIDs  map[string]int
	dirs     map[string]stagedInfo
	children map[string][]stagedDirEntry
}

func newLogicalTree() *logicalTree {
	tree := &logicalTree{files: make(map[string]renderwire.FileDescriptor), fileIDs: make(map[string]int), dirs: make(map[string]stagedInfo), children: make(map[string][]stagedDirEntry)}
	tree.dirs["."] = stagedInfo{name: ".", dir: true}
	tree.children["."] = nil
	return tree
}

func (tree *logicalTree) addDirectory(name string) {
	info := stagedInfo{name: path.Base(name), dir: true}
	tree.dirs[name] = info
	if _, exists := tree.children[name]; !exists {
		tree.children[name] = nil
	}
	parent := path.Dir(name)
	tree.children[parent] = append(tree.children[parent], stagedDirEntry{info: info})
}

func (tree *logicalTree) addFile(descriptor renderwire.FileDescriptor, index int) {
	tree.files[descriptor.Path] = descriptor
	tree.fileIDs[descriptor.Path] = index
	info := stagedInfo{name: path.Base(descriptor.Path), size: descriptor.Size}
	tree.children[path.Dir(descriptor.Path)] = append(tree.children[path.Dir(descriptor.Path)], stagedDirEntry{info: info, file: true})
}

func (tree *logicalTree) sortChildren() {
	for name := range tree.children {
		sort.Slice(tree.children[name], func(i, j int) bool { return tree.children[name][i].info.name < tree.children[name][j].info.name })
	}
}

type treeFS struct {
	stage *StagedBundle
	tree  *logicalTree
}

func (view treeFS) Open(name string) (fs.File, error) {
	if !validLogicalPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	view.stage.mu.Lock()
	defer view.stage.mu.Unlock()
	if view.stage.closed {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrClosed}
	}
	if descriptor, ok := view.tree.files[name]; ok {
		return view.stage.openDescriptorUnlocked(descriptor, view.tree.fileIDs[name])
	}
	if info, ok := view.tree.dirs[name]; ok {
		return &logicalDirFile{view: view, name: name, info: info}, nil
	}
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

func (s *StagedBundle) openDescriptorUnlocked(descriptor renderwire.FileDescriptor, index int) (fs.File, error) {
	file, err := os.Open(filepath.Join(s.root, payloadDir, opaqueName(index)))
	if err != nil {
		return nil, err
	}
	opened := &verifiedFile{stage: s, file: file, info: stagedInfo{name: path.Base(descriptor.Path), size: descriptor.Size}, remaining: descriptor.Size}
	copy(opened.expected[:], mustDecodeHash(descriptor.SHA256))
	s.open[opened] = struct{}{}
	return opened, nil
}

func (view treeFS) Stat(name string) (fs.FileInfo, error) {
	if !validLogicalPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	view.stage.mu.RLock()
	defer view.stage.mu.RUnlock()
	if view.stage.closed {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrClosed}
	}
	if descriptor, ok := view.tree.files[name]; ok {
		return stagedInfo{name: path.Base(name), size: descriptor.Size}, nil
	}
	if info, ok := view.tree.dirs[name]; ok {
		return info, nil
	}
	return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
}

func (view treeFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if !validLogicalPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	view.stage.mu.RLock()
	defer view.stage.mu.RUnlock()
	if view.stage.closed {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrClosed}
	}
	entries, ok := view.tree.children[name]
	if !ok {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}
	out := make([]fs.DirEntry, len(entries))
	for i, entry := range entries {
		out[i] = entry
	}
	return out, nil
}

type logicalDirFile struct {
	view   treeFS
	name   string
	info   stagedInfo
	closed bool
	offset int
}

func (file *logicalDirFile) Read([]byte) (int, error) { return 0, errors.New("read on directory") }
func (file *logicalDirFile) Stat() (fs.FileInfo, error) {
	if file.closed {
		return nil, fs.ErrClosed
	}
	return file.view.Stat(file.name)
}
func (file *logicalDirFile) Close() error { file.closed = true; return nil }
func (file *logicalDirFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if file.closed {
		return nil, fs.ErrClosed
	}
	entries, err := file.view.ReadDir(file.name)
	if err != nil {
		return nil, err
	}
	if file.offset >= len(entries) {
		return nil, io.EOF
	}
	if n <= 0 {
		out := entries[file.offset:]
		file.offset = len(entries)
		return out, nil
	}
	end := file.offset + n
	if end > len(entries) {
		end = len(entries)
	}
	out := entries[file.offset:end]
	file.offset = end
	return out, nil
}

type stagedInfo struct {
	name string
	size int64
	dir  bool
}

func (info stagedInfo) Name() string { return info.name }
func (info stagedInfo) Size() int64  { return info.size }
func (info stagedInfo) Mode() fs.FileMode {
	if info.dir {
		return fs.ModeDir | 0o555
	}
	return 0o444
}
func (info stagedInfo) ModTime() time.Time { return time.Time{} }
func (info stagedInfo) IsDir() bool        { return info.dir }
func (info stagedInfo) Sys() any           { return nil }

type stagedDirEntry struct {
	info stagedInfo
	file bool
}

func (entry stagedDirEntry) Name() string               { return entry.info.name }
func (entry stagedDirEntry) IsDir() bool                { return entry.info.dir }
func (entry stagedDirEntry) Type() fs.FileMode          { return entry.info.Mode().Type() }
func (entry stagedDirEntry) Info() (fs.FileInfo, error) { return entry.info, nil }

type verifiedFile struct {
	stage     *StagedBundle
	file      *os.File
	info      stagedInfo
	remaining int64
	bytes     int64
	expected  [32]byte
	hash      hash.Hash
	closed    bool
	verified  bool
	err       error
	mu        sync.Mutex
}

func (file *verifiedFile) Read(p []byte) (int, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.closed {
		return 0, fs.ErrClosed
	}
	if file.err != nil {
		return 0, file.err
	}
	if file.hash == nil {
		file.hash = sha256.New()
	}
	if file.remaining == 0 {
		return 0, file.verifyEOF()
	}
	if int64(len(p)) > file.remaining {
		p = p[:file.remaining]
	}
	n, err := file.file.Read(p)
	if n > 0 {
		_, _ = file.hash.Write(p[:n])
		file.remaining -= int64(n)
		file.bytes += int64(n)
	}
	if err == io.EOF {
		if verifyErr := file.verifyEOF(); verifyErr != nil {
			return n, verifyErr
		}
		return n, io.EOF
	}
	if err != nil {
		file.err = err
		return n, err
	}
	if file.remaining == 0 {
		// The next read detects extra bytes and finalizes the digest without ever
		// reading beyond the manifest-declared size into the caller's buffer.
	}
	return n, nil
}

func (file *verifiedFile) verifyEOF() error {
	if file.verified {
		return io.EOF
	}
	var extra [1]byte
	if n, err := file.file.Read(extra[:]); n != 0 || err != io.EOF {
		file.err = errors.New("staged payload exceeds its manifest size")
		return file.err
	}
	if file.bytes != file.info.size || !bytesEqual(file.hash.Sum(nil), file.expected[:]) {
		file.err = errors.New("staged payload failed size or SHA-256 verification")
		return file.err
	}
	file.verified = true
	return io.EOF
}

func (file *verifiedFile) Stat() (fs.FileInfo, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.closed {
		return nil, fs.ErrClosed
	}
	return file.info, nil
}

func (file *verifiedFile) Close() error {
	file.stage.mu.Lock()
	defer file.stage.mu.Unlock()
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.closed {
		return nil
	}
	file.closed = true
	delete(file.stage.open, file)
	return file.file.Close()
}

func verifyPayload(ctx context.Context, native string, descriptor renderwire.FileDescriptor) error {
	if err := requirePrivateRegular(native, descriptor.Size); err != nil {
		return err
	}
	file, err := os.Open(native)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != descriptor.Size {
		return errors.New("staged payload size or file type differs from manifest")
	}
	h := sha256.New()
	count, err := copyContext(ctx, h, file)
	if err != nil {
		return err
	}
	if count != descriptor.Size || hex.EncodeToString(h.Sum(nil)) != descriptor.SHA256 {
		return errors.New("staged payload hash differs from manifest")
	}
	return nil
}

func privateParent(name string) (string, error) {
	if name == "" {
		name = os.TempDir()
	}
	absolute, err := filepath.Abs(name)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("bundle temporary parent must be a directory")
	}
	return filepath.Clean(absolute), nil
}

func newToken() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(random[:]), nil
}

func opaqueName(index int) string { return fmt.Sprintf("%s%08x", filePrefix, index) }

func writePrivateFile(name string, data []byte) error {
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(writeErr, syncErr, closeErr)
}

func readBounded(name string, limit int64) ([]byte, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("staged metadata exceeds size limit")
	}
	return data, nil
}

func requirePrivateRegular(name string, limit int64) error {
	info, err := os.Lstat(name)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !privatePermissions(info.Mode()) || info.Size() < 0 || info.Size() > limit {
		return fmt.Errorf("staged file %q is not a bounded private regular file", filepath.Base(name))
	}
	return nil
}

func requirePrivateDirectory(name string) error {
	info, err := os.Lstat(name)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !privatePermissions(info.Mode()) {
		return errors.New("staged payload directory is not private")
	}
	return nil
}

func privatePermissions(mode fs.FileMode) bool {
	// Windows reports ACL-derived mode bits rather than the chmod bits passed
	// to CreateTemp/Mkdir. Runtime containers use Linux, where the check is strict.
	return runtime.GOOS == "windows" || mode.Perm()&0o077 == 0
}

func copyContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 64<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, readErr := source.Read(buffer)
		if n > 0 {
			written, writeErr := destination.Write(buffer[:n])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return total, nil
			}
			return total, readErr
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
}

func mustDecodeHash(value string) []byte {
	decoded, _ := hex.DecodeString(value)
	return decoded
}

func cloneManifest(manifest storagev1.SnapshotManifest) storagev1.SnapshotManifest {
	manifest.Files = append([]storagev1.SnapshotFile{}, manifest.Files...)
	manifest.Directories = append([]string{}, manifest.Directories...)
	manifest.Fonts = append([]storagev1.SnapshotFont{}, manifest.Fonts...)
	for i := range manifest.Fonts {
		manifest.Fonts[i].Files = append([]storagev1.SnapshotFile{}, manifest.Fonts[i].Files...)
		manifest.Fonts[i].Directories = append([]string{}, manifest.Fonts[i].Directories...)
	}
	return manifest
}

func validLogicalPath(name string) bool { return name == "." || renderwire.SafeLogicalPath(name) }

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var _ fs.FS = treeFS{}
var _ fs.StatFS = treeFS{}
var _ fs.ReadDirFS = treeFS{}
var _ fs.ReadDirFile = (*logicalDirFile)(nil)
var _ fs.File = (*verifiedFile)(nil)
