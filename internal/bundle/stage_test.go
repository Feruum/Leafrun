package bundle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	documentcore "github.com/leafrun/leafrun-document-core"
)

func fixture(t *testing.T) renderwire.Bundle {
	t.Helper()
	project := fstest.MapFS{
		"main.typ":           {Data: []byte("#include \"sections/body.typ\"\n")},
		"sections/body.typ":  {Data: []byte("= body\n")},
		"assets/logo.svg":    {Data: []byte("<svg/>")},
		"empty/nested/.keep": {Data: []byte{}},
	}
	font := fstest.MapFS{"font.otf": {Data: []byte("font bytes")}, "empty/.keep": {Data: []byte{}}}
	manifest := storagev1.SnapshotManifest{
		FormatVersion: 1,
		Origin:        storagev1.SourceOrigin{Kind: "folder", Location: "removed/source"},
		Files: []storagev1.SnapshotFile{
			fileEntry("assets/logo.svg", []byte("<svg/>")),
			fileEntry("empty/nested/.keep", nil),
			fileEntry("main.typ", []byte("#include \"sections/body.typ\"\n")),
			fileEntry("sections/body.typ", []byte("= body\n")),
		},
		Directories: []string{"assets", "empty", "empty/nested", "sections"},
		Fonts: []storagev1.SnapshotFont{{Path: ".", SourcePath: "/removed/font/path", InProject: false,
			Files:       []storagev1.SnapshotFile{fileEntry("empty/.keep", nil), fileEntry("font.otf", []byte("font bytes"))},
			Directories: []string{"empty"},
		}},
	}
	id, err := storagev1.ManifestContentID(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = id
	return renderwire.Bundle{Manifest: manifest, ProjectFS: project, ExternalFontFS: map[int]fs.FS{0: font}}
}

func fileEntry(name string, data []byte) storagev1.SnapshotFile {
	sum := sha256.Sum256(data)
	return storagev1.SnapshotFile{Path: name, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
}

func encodedBundle(t *testing.T, bundle renderwire.Bundle, terminal bool) *bytes.Buffer {
	t.Helper()
	wire := new(bytes.Buffer)
	e := renderwire.NewEncoder(wire, renderwire.InputLimit)
	if err := renderwire.WriteBundle(e, bundle); err != nil {
		t.Fatal(err)
	}
	if terminal {
		if err := e.WriteBytes(renderwire.Terminal, []byte(`{"complete":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	return wire
}

func TestStageStreamsOpaqueBundleAndReopensByToken(t *testing.T) {
	root := t.TempDir()
	wire := encodedBundle(t, fixture(t), false)
	staged, err := Stage(context.Background(), renderwire.NewDecoder(wire, renderwire.InputLimit), Options{TempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	token := staged.Token()
	if token == "" || strings.ContainsAny(token, `/\\`) {
		t.Fatalf("unsafe stage token %q", token)
	}
	owned := staged.root
	for _, descriptor := range mustPlan(t, staged.Manifest()) {
		file, err := staged.OpenVerified(descriptor)
		if err != nil {
			t.Fatal(err)
		}
		info, err := file.Stat()
		if err != nil || info.Size() != descriptor.Size {
			t.Fatalf("descriptor %s stat = %v, %v", descriptor.Path, info, err)
		}
		if _, err = io.Copy(io.Discard, file); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	project := staged.ProjectFS()
	if entries, err := fs.ReadDir(project, "empty/nested"); err != nil || len(entries) != 1 || entries[0].Name() != ".keep" {
		t.Fatalf("project empty directory listing = %v, %v", entries, err)
	}
	font, err := staged.FontFS(0)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := fs.ReadFile(font, "font.otf"); err != nil || string(b) != "font bytes" {
		t.Fatalf("external font = %q, %v", b, err)
	}
	if _, err := staged.FontFS(1); err == nil {
		t.Fatal("returned a font tree outside the manifest")
	}
	reopened, err := Reopen(context.Background(), root, token)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Manifest().ID != staged.Manifest().ID {
		t.Fatal("reopened manifest changed")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(owned); !os.IsNotExist(err) {
		t.Fatalf("Close did not remove its owned root: %v", err)
	}
}

func mustPlan(t *testing.T, manifest storagev1.SnapshotManifest) []renderwire.FileDescriptor {
	t.Helper()
	files, err := renderwire.PlanFiles(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestStageCleansPartialImportAndRejectsBrokenFrame(t *testing.T) {
	root := t.TempDir()
	wire := encodedBundle(t, fixture(t), false).Bytes()
	corrupt := bytes.Clone(wire)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := Stage(context.Background(), renderwire.NewDecoder(bytes.NewReader(corrupt), renderwire.InputLimit), Options{TempDir: root}); err == nil {
		t.Fatal("accepted a corrupt final bundle frame")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("partial import leaked private staging: entries=%v err=%v", entries, err)
	}
}

func TestStageStreamsLargeFrameThroughPipe(t *testing.T) {
	const size = 8 << 20
	root := t.TempDir()
	sourceRoot := t.TempDir()
	source, err := os.Create(filepath.Join(sourceRoot, "large.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	zeroes := make([]byte, 64<<10)
	digest := sha256.New()
	for remaining := size; remaining > 0; remaining -= len(zeroes) {
		_, _ = digest.Write(zeroes)
	}
	manifest := storagev1.SnapshotManifest{
		FormatVersion: 1,
		Origin:        storagev1.SourceOrigin{Kind: "folder", Location: "removed/source"},
		Files:         []storagev1.SnapshotFile{{Path: "large.bin", Size: size, SHA256: hex.EncodeToString(digest.Sum(nil))}},
		Directories:   []string{},
		Fonts:         []storagev1.SnapshotFont{},
	}
	manifest.ID, err = storagev1.ManifestContentID(manifest)
	if err != nil {
		t.Fatal(err)
	}
	reader, writer := io.Pipe()
	writeDone := make(chan error, 1)
	go func() {
		encoder := renderwire.NewEncoder(writer, renderwire.InputLimit)
		writeErr := renderwire.WriteBundle(encoder, renderwire.Bundle{Manifest: manifest, ProjectFS: os.DirFS(sourceRoot)})
		_ = writer.CloseWithError(writeErr)
		writeDone <- writeErr
	}()
	staged, err := Stage(context.Background(), renderwire.NewDecoder(reader, renderwire.InputLimit), Options{TempDir: root})
	if err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		_ = staged.Close()
		t.Fatal("write bundle stream:", err)
	}
	defer staged.Close()
	project := staged.ProjectFS()
	file, err := project.Open("large.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	count, err := io.Copy(io.Discard, file)
	if err != nil || count != size {
		t.Fatalf("staged stream size=%d, want %d: %v", count, size, err)
	}
}

func TestStageFSIsLogicalAndInvalidAfterClose(t *testing.T) {
	root := t.TempDir()
	staged, err := Stage(context.Background(), renderwire.NewDecoder(encodedBundle(t, fixture(t), false), renderwire.InputLimit), Options{TempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(staged.ProjectFS(), "../main.typ"); err == nil {
		t.Fatal("accepted path traversal")
	}
	view := staged.ProjectFS()
	if err := staged.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(view, "main.typ"); err == nil {
		t.Fatal("FS remained usable after Close")
	}
	if err := staged.Close(); err != nil {
		t.Fatal("Close must be idempotent:", err)
	}
}

func TestStageRechecksFilesAndRejectsSymlinkOnReopen(t *testing.T) {
	root := t.TempDir()
	staged, err := Stage(context.Background(), renderwire.NewDecoder(encodedBundle(t, fixture(t), false), renderwire.InputLimit), Options{TempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	token := staged.Token()
	path := filepath.Join(staged.root, "payload", "f00000000")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := Reopen(context.Background(), root, token); err == nil {
		t.Fatal("reopened an opaque payload symlink")
	}
	_ = staged.Close()
}

func TestReopenRejectsChangedPayloadAndManifest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *StagedBundle)
	}{
		{
			name: "payload hash",
			mutate: func(t *testing.T, staged *StagedBundle) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(staged.root, "payload", "f00000000"), []byte("<bad/>"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "manifest marker",
			mutate: func(t *testing.T, staged *StagedBundle) {
				t.Helper()
				manifestPath := filepath.Join(staged.root, manifestFile)
				data, err := os.ReadFile(manifestPath)
				if err != nil {
					t.Fatal(err)
				}
				data[0] ^= 1
				if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			staged, err := Stage(context.Background(), renderwire.NewDecoder(encodedBundle(t, fixture(t), false), renderwire.InputLimit), Options{TempDir: root})
			if err != nil {
				t.Fatal(err)
			}
			defer staged.Close()
			tc.mutate(t, staged)
			if _, err := Reopen(context.Background(), root, staged.Token()); err == nil {
				t.Fatal("reopened modified stage")
			}
		})
	}
}

func TestOpenVerifiedRejectsPayloadChangedAfterImport(t *testing.T) {
	staged, err := Stage(context.Background(), renderwire.NewDecoder(encodedBundle(t, fixture(t), false), renderwire.InputLimit), Options{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	descriptor := mustPlan(t, staged.Manifest())[0]
	if err := os.WriteFile(filepath.Join(staged.root, payloadDir, opaqueName(0)), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := staged.OpenVerified(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := io.Copy(io.Discard, file); err == nil {
		t.Fatal("streaming reader accepted payload modified after import")
	}
}

func TestStageBundleCanBeRestoredByCore(t *testing.T) {
	root := t.TempDir()
	staged, err := Stage(context.Background(), renderwire.NewDecoder(encodedBundle(t, fixture(t), false), renderwire.InputLimit), Options{TempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	manifest := staged.Manifest()
	coreManifest := toCoreManifest(manifest)
	external := map[int]fs.FS{0: staged.FontFSMust(0)}
	base, err := documentcore.RestoreSnapshot(context.Background(), documentcore.SnapshotBundle{
		Manifest: coreManifest, ProjectFS: staged.ProjectFS(), ExternalFontFS: external,
	}, documentcore.RestoreSnapshotOptions{TempDir: root, Limits: documentcore.SnapshotLimits{MaxFiles: 10000, MaxTotalBytes: 100 << 20, MaxFileBytes: 100 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	if base.Manifest().ID != manifest.ID {
		t.Fatal("restored base ID changed")
	}
}

func (staged *StagedBundle) FontFSMust(index int) fs.FS {
	// This helper is intentionally in the test file, and disappears from the API.
	view, err := staged.FontFS(index)
	if err != nil {
		panic(err)
	}
	return view
}

func toCoreManifest(m storagev1.SnapshotManifest) documentcore.SnapshotManifest {
	files := func(in []storagev1.SnapshotFile) []documentcore.SnapshotFile {
		out := make([]documentcore.SnapshotFile, len(in))
		for i, f := range in {
			out[i] = documentcore.SnapshotFile{Path: f.Path, Size: f.Size, SHA256: f.SHA256}
		}
		return out
	}
	fonts := make([]documentcore.SnapshotFont, len(m.Fonts))
	for i, f := range m.Fonts {
		fonts[i] = documentcore.SnapshotFont{Path: f.Path, SourcePath: f.SourcePath, InProject: f.InProject, Files: files(f.Files), Directories: append([]string{}, f.Directories...)}
	}
	return documentcore.SnapshotManifest{
		FormatVersion: m.FormatVersion, ID: m.ID,
		Origin: documentcore.SourceOrigin{Kind: m.Origin.Kind, Location: m.Origin.Location, SourceRoot: m.Origin.SourceRoot, Ref: m.Origin.Ref, Revision: m.Origin.Revision, ArchiveSHA256: m.Origin.ArchiveSHA256},
		Files:  files(m.Files), Directories: append([]string{}, m.Directories...), Fonts: fonts,
	}
}
