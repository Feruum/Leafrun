package sandbox

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
)

// WriteInput inserts exact normalized parameter bytes into the request frame.
// It never routes them through an encoder or a numeric value representation.
func WriteInput(w io.Writer, request renderwire.RenderRequest, source renderwire.Bundle, openFiles ...func(projectsv1.PreparedFile) (io.ReadCloser, error)) error {
	if len(openFiles) > 1 {
		return errors.New("at most one input-file opener is allowed")
	}
	parameters := request.Parameters
	if len(parameters) > 0 {
		normalized, err := jsonvalue.NormalizeObject(parameters)
		if err != nil {
			return err
		}
		if !bytes.Equal(normalized, parameters) {
			return errors.New("parameters must already be normalized")
		}
	}
	request.Parameters = nil
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	if len(parameters) > 0 {
		data = append(data[:len(data)-1], []byte(`,"parameters":`)...)
		data = append(data, parameters...)
		data = append(data, '}')
	}
	e := renderwire.NewEncoder(w, renderwire.InputLimit)
	if err = e.WriteBytes(renderwire.Request, data); err != nil {
		return err
	}
	if err = renderwire.WriteBundle(e, source); err != nil {
		return err
	}
	if request.Execution != nil {
		var open func(projectsv1.PreparedFile) (io.ReadCloser, error)
		if len(openFiles) == 1 {
			open = openFiles[0]
		}
		if err = renderwire.WriteInputFiles(e, request.Execution.Prepared.Files, open); err != nil {
			return err
		}
	} else if len(openFiles) == 1 && openFiles[0] != nil {
		return errors.New("input-file opener requires a prepared execution")
	}
	return e.WriteBytes(renderwire.Terminal, []byte(`{"complete":true}`))
}

// DockerStdout decodes Docker's non-TTY multiplexing without buffering a whole
// block. Stdout has the framed protocol limit; stderr is discarded after a
// separate bounded count. Docker logging is disabled on container creation.
type DockerStdout struct {
	source         io.Reader
	left           int64
	stdout, stderr int64
	err            error
}

func NewDockerStdout(source io.Reader) *DockerStdout { return &DockerStdout{source: source} }
func (r *DockerStdout) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	for r.left == 0 {
		var header [8]byte
		_, err := io.ReadFull(r.source, header[:])
		if err != nil {
			r.err = err
			return 0, err
		}
		if header[1] != 0 || header[2] != 0 || header[3] != 0 {
			r.err = errors.New("invalid Docker output header")
			return 0, r.err
		}
		size := int64(binary.BigEndian.Uint32(header[4:]))
		switch header[0] {
		case 1:
			if size > renderwire.OutputLimit-r.stdout {
				r.err = errors.New("Docker stdout exceeds output limit")
				return 0, r.err
			}
			r.stdout += size
			r.left = size
		case 2:
			if size > (64<<10)-r.stderr {
				r.err = errors.New("Docker stderr exceeds diagnostic limit")
				return 0, r.err
			}
			r.stderr += size
			if _, err = io.CopyN(io.Discard, r.source, size); err != nil {
				r.err = err
				return 0, err
			}
		default:
			r.err = errors.New("unexpected Docker stream kind")
			return 0, r.err
		}
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, err := r.source.Read(p)
	r.left -= int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) && r.left != 0 {
			err = io.ErrUnexpectedEOF
		}
		r.err = err
	}
	return n, err
}
