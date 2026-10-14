package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

type snapshotFileKey struct {
	tree      string
	fontIndex int
	path      string
}

// remoteSnapshotBundle exposes only the logical files declared by an immutable
// Storage snapshot. It never opens manifest paths as native filesystem names.
type remoteSnapshotBundle struct {
	ctx         context.Context
	storage     Storage
	manifest    storagev1.SnapshotManifest
	resources   map[snapshotFileKey]string
	descriptors map[snapshotFileKey]renderwire.FileDescriptor
	mu          sync.Mutex
	err         error
}

func newRemoteSnapshotBundle(ctx context.Context, storage Storage, snapshot storagev1.Snapshot, job generationJobIdentity) (*remoteSnapshotBundle, renderwire.Bundle, error) {
	if snapshot.SnapshotID != job.SnapshotID || snapshot.Scope != job.Scope || snapshot.Manifest.ID == "" || len(snapshot.Manifest.ID) > 256 {
		return nil, renderwire.Bundle{}, errors.New("source snapshot identity or scope differs from the job")
	}
	files, err := renderwire.PlanFiles(snapshot.Manifest)
	if err != nil {
		return nil, renderwire.Bundle{}, fmt.Errorf("source snapshot manifest is invalid: %w", err)
	}
	if job.ContentID != "" && job.ContentID != snapshot.Manifest.ID {
		return nil, renderwire.Bundle{}, errors.New("source snapshot content ID differs from the durable job")
	}
	if len(snapshot.Resources) != len(files) {
		return nil, renderwire.Bundle{}, errors.New("source snapshot resource references differ from its manifest")
	}
	byFile := make(map[snapshotFileKey]renderwire.FileDescriptor, len(files))
	for _, file := range files {
		byFile[snapshotFileKey{tree: file.Tree, fontIndex: file.FontIndex, path: file.Path}] = file
	}
	resources := make(map[snapshotFileKey]string, len(files))
	for _, ref := range snapshot.Resources {
		key := snapshotFileKey{tree: ref.Tree, fontIndex: ref.FontIndex, path: ref.Path}
		if _, ok := byFile[key]; !ok || ref.ResourceID == "" || len(ref.ResourceID) > 256 || strings.ContainsAny(ref.ResourceID, "\x00\r\n") {
			return nil, renderwire.Bundle{}, errors.New("source snapshot contains an unknown or invalid resource reference")
		}
		if _, duplicate := resources[key]; duplicate {
			return nil, renderwire.Bundle{}, errors.New("source snapshot contains a duplicate resource reference")
		}
		resources[key] = ref.ResourceID
	}
	if len(resources) != len(byFile) {
		return nil, renderwire.Bundle{}, errors.New("source snapshot omits a manifest resource")
	}
	remote := &remoteSnapshotBundle{ctx: ctx, storage: storage, manifest: snapshot.Manifest, resources: resources, descriptors: byFile}
	bundle := renderwire.Bundle{Manifest: snapshot.Manifest, ProjectFS: remoteSnapshotFS{bundle: remote, tree: "project"}, ExternalFontFS: make(map[int]fs.FS)}
	for index, font := range snapshot.Manifest.Fonts {
		if !font.InProject {
			bundle.ExternalFontFS[index] = remoteSnapshotFS{bundle: remote, tree: "font", fontIndex: index}
		}
	}
	return remote, bundle, nil
}

type generationJobIdentity struct {
	SnapshotID string
	ContentID  string
	Scope      storagev1.Scope
}

type remoteSnapshotFS struct {
	bundle    *remoteSnapshotBundle
	tree      string
	fontIndex int
}

func (view remoteSnapshotFS) Open(name string) (fs.File, error) {
	if view.bundle == nil || !renderwire.SafeLogicalPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	key := snapshotFileKey{tree: view.tree, fontIndex: view.fontIndex, path: name}
	resourceID, ok := view.bundle.resources[key]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	if err := view.bundle.ctx.Err(); err != nil {
		return nil, err
	}
	file, ok := view.bundle.descriptors[key]
	if !ok {
		return nil, errors.New("source descriptor disappeared from its immutable manifest")
	}
	response, err := view.bundle.storage.ResourceContent(view.bundle.ctx, resourceID)
	if err != nil {
		return nil, errors.New("source resource could not be opened")
	}
	if response == nil || response.Body == nil {
		return nil, errors.New("Storage returned an empty source response")
	}
	if response.StatusCode != http.StatusOK || (response.ContentLength >= 0 && response.ContentLength != file.Size) {
		_ = response.Body.Close()
		return nil, errors.New("Storage source response status or length differs from the manifest")
	}
	return &remoteSnapshotFile{bundle: view.bundle, body: response.Body, name: path.Base(name), size: file.Size, remaining: file.Size}, nil
}

// Verify reports response-body errors discovered by Close. The renderwire
// encoder verifies every declared size and SHA-256 while it streams; Close
// additionally rejects bytes beyond the manifest's declared size.
func (b *remoteSnapshotBundle) Verify() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

func (b *remoteSnapshotBundle) record(err error) {
	if err == nil {
		return
	}
	b.mu.Lock()
	if b.err == nil {
		b.err = err
	}
	b.mu.Unlock()
}

type remoteSnapshotFile struct {
	bundle    *remoteSnapshotBundle
	body      io.ReadCloser
	name      string
	size      int64
	remaining int64
	closed    bool
	verified  bool
}

func (f *remoteSnapshotFile) Read(p []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := f.bundle.ctx.Err(); err != nil {
		return 0, err
	}
	if f.remaining == 0 {
		return 0, f.verifyEnd()
	}
	if int64(len(p)) > f.remaining {
		p = p[:f.remaining]
	}
	n, err := f.body.Read(p)
	f.remaining -= int64(n)
	if err == io.EOF && f.remaining > 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func (f *remoteSnapshotFile) verifyEnd() error {
	if f.verified {
		return io.EOF
	}
	var one [1]byte
	for {
		if err := f.bundle.ctx.Err(); err != nil {
			f.bundle.record(err)
			return err
		}
		n, err := f.body.Read(one[:])
		if n > 0 {
			f.bundle.record(errors.New("Storage source resource exceeds its declared size"))
			return errors.New("Storage source resource exceeds its declared size")
		}
		if errors.Is(err, io.EOF) {
			f.verified = true
			return io.EOF
		}
		if err != nil {
			f.bundle.record(err)
			return err
		}
	}
}

func (f *remoteSnapshotFile) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, fs.ErrClosed
	}
	return remoteFileInfo{name: f.name, size: f.size}, nil
}

func (f *remoteSnapshotFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	var err error
	if f.remaining != 0 {
		err = errors.New("Storage source resource ended before its declared size")
	} else if !f.verified {
		err = f.verifyEnd()
	}
	closeErr := f.body.Close()
	if err == io.EOF {
		err = nil
	}
	err = errors.Join(err, closeErr)
	f.bundle.record(err)
	return err
}

type remoteFileInfo struct {
	name string
	size int64
}

func (i remoteFileInfo) Name() string     { return i.name }
func (i remoteFileInfo) Size() int64      { return i.size }
func (remoteFileInfo) Mode() fs.FileMode  { return 0o400 }
func (remoteFileInfo) ModTime() time.Time { return time.Time{} }
func (remoteFileInfo) IsDir() bool        { return false }
func (remoteFileInfo) Sys() any           { return nil }

var _ fs.FS = remoteSnapshotFS{}
var _ fs.File = (*remoteSnapshotFile)(nil)
