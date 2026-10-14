package sandbox

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"testing"
	"testing/fstest"

	"github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestInputPreservesExactNormalizedParameterBytes(t *testing.T) {
	parameters, err := jsonvalue.NormalizeObject([]byte(`{"float":1.0,"integer":9007199254740993,"html":"<>&","exponent":1e0}`))
	if err != nil {
		t.Fatal(err)
	}
	m := storagev1.SnapshotManifest{FormatVersion: 1, Files: []storagev1.SnapshotFile{}, Directories: []string{}, Fonts: []storagev1.SnapshotFont{}}
	m.ID, err = storagev1.ManifestContentID(m)
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if err = WriteInput(&wire, renderwire.RenderRequest{Parameters: parameters}, renderwire.Bundle{Manifest: m, ProjectFS: fstest.MapFS{}}); err != nil {
		t.Fatal(err)
	}
	d := renderwire.NewDecoder(&wire, renderwire.InputLimit)
	data, err := d.ReadBytes(renderwire.Request)
	if err != nil {
		t.Fatal(err)
	}
	var request renderwire.RenderRequest
	if err = json.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(request.Parameters, parameters) {
		t.Fatalf("parameters reserialized: got %s want %s", request.Parameters, parameters)
	}
}

func dockerBlock(kind byte, data []byte) []byte {
	header := make([]byte, 8)
	header[0] = kind
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	return append(header, data...)
}
func TestDockerDemultiplexingPreservesBinaryAndBoundsStderr(t *testing.T) {
	data := append(dockerBlock(2, []byte("safe stderr")), dockerBlock(1, []byte{'L', 0, 255})...)
	data = append(data, dockerBlock(1, []byte{3, 4})...)
	actual, err := io.ReadAll(NewDockerStdout(bytes.NewReader(data)))
	if err != nil || !bytes.Equal(actual, []byte{'L', 0, 255, 3, 4}) {
		t.Fatalf("binary stdout: %v %v", actual, err)
	}
	if _, err = io.ReadAll(NewDockerStdout(bytes.NewReader(dockerBlock(2, make([]byte, (64<<10)+1))))); err == nil {
		t.Fatal("unbounded stderr accepted")
	}
	if _, err = io.ReadAll(NewDockerStdout(bytes.NewReader(dockerBlock(1, []byte("partial"))[:10]))); err == nil {
		t.Fatal("partial Docker block accepted")
	}
}
