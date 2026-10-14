package sandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/Feruum/Leafrun/internal/bundle"
	"github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
)

var outputToken = regexp.MustCompile(`^result-[a-zA-Z0-9]+$`)

type Output struct {
	Terminal      renderwire.TerminalEnvelope
	Derived       *bundle.StagedBundle
	root, pdfPath string
	complete      bool
}

func (o *Output) Token() string { return filepath.Base(o.root) }
func (o *Output) Close() error {
	if o == nil {
		return nil
	}
	var err error
	if o.Derived != nil {
		err = o.Derived.Close()
	}
	return errors.Join(err, os.RemoveAll(o.root))
}

type outputProof struct {
	FormatVersion int                         `json:"formatVersion"`
	Spec          Spec                        `json:"spec"`
	ContainerID   string                      `json:"containerId"`
	Terminal      renderwire.TerminalEnvelope `json:"terminal"`
	DerivedToken  string                      `json:"derivedToken,omitempty"`
}

func safeOutputParent(parent string) error {
	if !filepath.IsAbs(parent) {
		return errors.New("private output parent must be absolute")
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("private output parent is not an ordinary directory")
	}
	return nil
}

// ReadOutput streams while the renderer is alive. It never creates the
// confirmation marker; full frames and EOF alone do not establish exit status.
func ReadOutput(ctx context.Context, r io.Reader, parent string, expected renderwire.RenderRequest) (_ *Output, err error) {
	if err = safeOutputParent(parent); err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp(parent, "result-")
	if err != nil {
		return nil, err
	}
	o := &Output{root: root, pdfPath: filepath.Join(root, "pdf")}
	defer func() {
		if err != nil {
			_ = o.Close()
		}
	}()
	d := renderwire.NewDecoder(contextReader{ctx, r}, renderwire.OutputLimit)
	kind, err := d.PeekKind()
	if err != nil {
		return nil, err
	}
	if kind == renderwire.Manifest {
		o.Derived, err = bundle.Stage(ctx, d, bundle.Options{TempDir: root})
		if err != nil {
			return nil, err
		}
		kind, err = d.PeekKind()
		if err != nil {
			return nil, err
		}
	}
	var descriptor *renderwire.PDFDescriptor
	if kind == renderwire.PDFHeader {
		data, e := d.ReadBytes(renderwire.PDFHeader)
		if e != nil {
			return nil, e
		}
		var pdf renderwire.PDFDescriptor
		if e = renderwire.DecodeJSON(data, &pdf); e != nil {
			return nil, e
		}
		hash, e := hex.DecodeString(pdf.SHA256)
		if e != nil || len(hash) != 32 || pdf.Size <= 0 || pdf.Size > renderwire.PDFLimit {
			return nil, errors.New("invalid PDF descriptor")
		}
		file, e := os.OpenFile(o.pdfPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return nil, e
		}
		e = d.Read(renderwire.PDF, func(header renderwire.Header, body io.Reader) error {
			if header.Size != pdf.Size || !bytes.Equal(header.SHA256[:], hash) {
				return errors.New("PDF frame differs from descriptor")
			}
			_, copyErr := io.Copy(file, body)
			return copyErr
		})
		if e == nil {
			e = file.Sync()
		}
		e = errors.Join(e, file.Close())
		if e != nil {
			return nil, e
		}
		descriptor = &pdf
	}
	data, err := d.ReadBytes(renderwire.Terminal)
	if err != nil {
		return nil, err
	}
	if err = renderwire.DecodeJSON(data, &o.Terminal); err != nil {
		return nil, err
	}
	if err = d.Finish(); err != nil {
		return nil, err
	}
	if err = validateOutput(o, expected, descriptor); err != nil {
		return nil, err
	}
	o.complete = true
	return o, nil
}

func validateOutput(o *Output, expected renderwire.RenderRequest, pdf *renderwire.PDFDescriptor) error {
	t := o.Terminal
	if !t.Complete || t.Scope != expected.Scope || t.JobID != expected.JobID || t.AttemptID != expected.AttemptID || t.TargetID != expected.TargetID || t.InputFingerprint != expected.InputFingerprint || t.SourceSnapshotID != expected.SourceSnapshotID || t.SourceContentID != expected.SourceContentID || t.EngineVersion != "0.15.1" {
		return errors.New("renderer terminal binding is invalid")
	}
	if o.Derived != nil {
		if t.DerivedContentID != o.Derived.Manifest().ID || len(expected.Parameters) == 0 {
			return errors.New("derived snapshot binding is invalid")
		}
	} else if t.DerivedContentID != "" || t.Success && len(expected.Parameters) > 0 {
		return errors.New("derived snapshot is missing")
	}
	if t.Success {
		if pdf == nil || t.PDF == nil || *pdf != *t.PDF || t.Phase != "complete" || t.FailureCode != "" {
			return errors.New("success terminal lacks verified PDF")
		}
	} else if pdf != nil || t.PDF != nil || t.FailureCode == "" || (t.Phase != "validation" && t.Phase != "compile" && t.Phase != "resource_limit") {
		return errors.New("failure terminal is inconsistent")
	}
	return nil
}

// Confirm durably records an independently inspected exact-ID exit 0. Upload
// callers must additionally commit the stopped/finished attempt in Generation.
func (o *Output) Confirm(spec Spec, state State) error {
	if !o.complete || !state.Successful() || !exactID.MatchString(state.ID) || spec.JobID != o.Terminal.JobID || spec.AttemptID != o.Terminal.AttemptID {
		return errors.New("output requires full stream and confirmed exact-container exit zero")
	}
	if _, err := creationOptions(spec); err != nil {
		return err
	}
	proof := outputProof{FormatVersion: 1, Spec: spec, ContainerID: state.ID, Terminal: o.Terminal}
	if o.Derived != nil {
		proof.DerivedToken = o.Derived.Token()
	}
	data, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	if len(data) > 2<<20 {
		return errors.New("confirmed output metadata exceeds limit")
	}
	name := filepath.Join(o.root, "confirmed.json")
	file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}

func ReopenOutput(ctx context.Context, parent, token string, expected renderwire.RenderRequest, spec Spec, containerID string) (_ *Output, err error) {
	if err = safeOutputParent(parent); err != nil {
		return nil, err
	}
	if !outputToken.MatchString(token) {
		return nil, errors.New("invalid private result token")
	}
	root := filepath.Join(parent, token)
	if err = safeOutputParent(root); err != nil {
		return nil, err
	}
	file, err := openRegular(root, "confirmed.json")
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, (2<<20)+1))
	err = errors.Join(err, file.Close())
	if err != nil {
		return nil, err
	}
	if len(data) > 2<<20 {
		return nil, errors.New("confirmation metadata exceeds limit")
	}
	var proof outputProof
	if err = jsonvalue.Unmarshal(data, &proof, 2<<20); err != nil {
		return nil, err
	}
	if proof.FormatVersion != 1 || proof.Spec != spec || proof.ContainerID != containerID || !exactID.MatchString(containerID) {
		return nil, errors.New("confirmed output identity differs")
	}
	o := &Output{root: root, pdfPath: filepath.Join(root, "pdf"), Terminal: proof.Terminal, complete: true}
	if proof.DerivedToken != "" {
		o.Derived, err = bundle.Reopen(ctx, root, proof.DerivedToken)
		if err != nil {
			return nil, err
		}
	}
	if o.Terminal.PDF != nil {
		file, err = o.OpenPDF()
		if err != nil {
			return nil, err
		}
		if err = file.Close(); err != nil {
			return nil, err
		}
	}
	if err = validateOutput(o, expected, o.Terminal.PDF); err != nil {
		return nil, err
	}
	return o, nil
}

func openRegular(root, name string) (*os.File, error) {
	info, err := os.Lstat(filepath.Join(root, name))
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("private output is not a regular file")
	}
	return os.Open(filepath.Join(root, name))
}
func (o *Output) OpenPDF() (*os.File, error) {
	if o.Terminal.PDF == nil {
		return nil, errors.New("output has no PDF")
	}
	file, err := openRegular(o.root, "pdf")
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(file, renderwire.PDFLimit+1))
	if err != nil || n != o.Terminal.PDF.Size || hex.EncodeToString(hash.Sum(nil)) != o.Terminal.PDF.SHA256 {
		file.Close()
		return nil, errors.New("staged PDF differs from confirmed bytes")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
