// Package renderer implements the stdin/stdout generation renderer protocol.
// It owns only private copies under WorkDir and has no storage or network API.
package renderer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Feruum/Leafrun/internal/bundle"
	"github.com/Feruum/Leafrun/internal/parameters"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	documentcore "github.com/leafrun/leafrun-document-core"
)

const (
	maxDiagnostics       = 128
	maxDiagnosticBytes   = 2048
	maxDiagnosticList    = 16
	maxRawDiagnosticSize = 96 << 10
)

var snapshotLimits = documentcore.SnapshotLimits{
	MaxFiles:      10000,
	MaxTotalBytes: renderwire.SnapshotLimit,
	MaxFileBytes:  renderwire.SnapshotLimit,
}

// Options selects the parent for all renderer-owned temporary files. The
// production command fixes this to /work; tests may supply a private temp dir.
type Options struct {
	WorkDir string
}

// Run consumes one complete request/base-snapshot/input-terminal stream and
// emits a terminal result. A handled validation or compile failure returns nil
// after writing a valid terminal envelope; wire and infrastructure errors
// return an error and leave any partial output unpublishable.
func Run(ctx context.Context, input io.Reader, output io.Writer, options Options) (runErr error) {
	if ctx == nil || input == nil || output == nil {
		return errors.New("renderer context, input and output are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	workDir, err := workDirectory(options.WorkDir)
	if err != nil {
		return err
	}
	workRoot, err := os.MkdirTemp(workDir, "leafrun-render-")
	if err != nil {
		return fmt.Errorf("create renderer work root: %w", err)
	}
	defer func() { runErr = errors.Join(runErr, os.RemoveAll(workRoot)) }()
	decoder := renderwire.NewDecoder(input, renderwire.InputLimit)
	requestBytes, err := decoder.ReadBytes(renderwire.Request)
	if err != nil {
		return fmt.Errorf("read render request frame: %w", err)
	}
	var request renderwire.RenderRequest
	if err := renderwire.DecodeJSON(requestBytes, &request); err != nil {
		return fmt.Errorf("decode render request: %w", err)
	}
	inputBundle, err := bundle.Stage(ctx, decoder, bundle.Options{TempDir: workRoot})
	if err != nil {
		return fmt.Errorf("stage input bundle: %w", err)
	}
	defer func() {
		if inputBundle != nil {
			runErr = errors.Join(runErr, inputBundle.Close())
		}
	}()
	stagedInputFiles := make(map[string]string)
	if request.Execution != nil {
		stagedInputFiles, err = stageInputFiles(ctx, decoder, workRoot, request.Execution.Prepared.Files)
		if err != nil {
			return fmt.Errorf("stage prepared input files: %w", err)
		}
	}
	inputTerminalBytes, err := decoder.ReadBytes(renderwire.Terminal)
	if err != nil {
		return fmt.Errorf("read input terminal frame: %w", err)
	}
	var inputTerminal renderwire.InputTerminal
	if err := renderwire.DecodeJSON(inputTerminalBytes, &inputTerminal); err != nil {
		return fmt.Errorf("decode input terminal: %w", err)
	}
	if !inputTerminal.Complete {
		return errors.New("input stream terminal is incomplete")
	}
	if err := decoder.Finish(); err != nil {
		return fmt.Errorf("finish input stream: %w", err)
	}
	encoder := renderwire.NewEncoder(output, renderwire.OutputLimit)
	terminal := terminalFor(request)
	manifest := inputBundle.Manifest()
	if requestErr := validateRequest(request, manifest); requestErr != nil {
		if err := inputBundle.Close(); err != nil {
			return errors.Join(requestErr, err)
		}
		inputBundle = nil
		return writeFailure(encoder, terminal, "validation", "invalid_request", "renderer request validation failed", nil, "")
	}
	baseBundle, err := inputBundle.Bundle()
	if err != nil {
		return err
	}
	base, err := documentcore.RestoreSnapshot(ctx, toCoreBundle(baseBundle), documentcore.RestoreSnapshotOptions{TempDir: workRoot, Limits: snapshotLimits})
	if err != nil {
		return fmt.Errorf("restore immutable source snapshot: %w", err)
	}
	defer func() {
		if base != nil {
			runErr = errors.Join(runErr, base.Close())
		}
	}()
	if err := inputBundle.Close(); err != nil {
		return fmt.Errorf("release imported input bundle: %w", err)
	}
	inputBundle = nil
	if request.Execution == nil && len(request.Parameters) > 0 {
		if err := parameters.Validate(ctx, base.FS(), request.ParametersSchemaPath, request.Parameters); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := base.Close(); err != nil {
				return errors.Join(err, errors.New("close source snapshot after parameter validation"))
			}
			base = nil
			return writeFailure(encoder, terminal, "validation", "parameters_invalid", "parameters do not satisfy the base snapshot schema", nil, "")
		}
	}

	var derived *documentcore.Snapshot
	defer func() {
		if derived != nil {
			runErr = errors.Join(runErr, derived.Close())
		}
	}()
	renderSnapshot := base
	pdfPath := filepath.Join(workRoot, "result.pdf")
	renderOptions := documentcore.RenderSnapshotOptions{Entrypoint: request.Entrypoint, Output: pdfPath}
	if request.Execution != nil {
		descriptor, preparedSnapshot, prepareErr := prepareSelection(ctx, base, request, stagedInputFiles, workRoot)
		if prepareErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if base != nil {
				if closeErr := base.Close(); closeErr != nil {
					return errors.Join(prepareErr, closeErr)
				}
				base = nil
			}
			var snapshotErr *documentcore.SnapshotError
			if errors.As(prepareErr, &snapshotErr) && snapshotErr.Stage != documentcore.SnapshotStageValidate {
				return fmt.Errorf("prepare frozen execution snapshot: %w", prepareErr)
			}
			return writeFailure(encoder, terminal, "validation", "prepared_input_invalid", "prepared input does not match the captured project", nil, "")
		}
		derived = preparedSnapshot
		renderSnapshot = derived
		renderOptions.Inputs = cloneStringMap(descriptor.ResolvedProfile.Inputs)
		renderOptions.Inputs["leafrun-data"] = "/input.json"
		renderOptions.PDFStandard = descriptor.ResolvedProfile.PDFStandard
		if closeErr := base.Close(); closeErr != nil {
			_ = derived.Close()
			derived = nil
			base = nil
			return fmt.Errorf("release source snapshot before rendering: %w", closeErr)
		}
		base = nil
	} else if len(request.Parameters) > 0 {
		derived, err = documentcore.CreateInputSnapshot(ctx, base, request.Parameters, documentcore.CreateInputSnapshotOptions{TempDir: workRoot, Limits: snapshotLimits})
		if err != nil {
			if derived != nil {
				_ = derived.Close()
			}
			var snapshotErr *documentcore.SnapshotError
			if errors.As(err, &snapshotErr) && snapshotErr.Stage == documentcore.SnapshotStageValidate {
				if closeErr := base.Close(); closeErr != nil {
					return errors.Join(err, closeErr)
				}
				base = nil
				failureCode := "parameters_invalid"
				message := "parameters could not be applied to the source snapshot"
				if errors.Is(err, documentcore.ErrSnapshotLimit) {
					failureCode = "resource_limit"
					message = "derived snapshot exceeds the renderer resource limit"
				}
				return writeFailure(encoder, terminal, "validation", failureCode, message, nil, "")
			}
			return fmt.Errorf("derive input snapshot: %w", err)
		}
		if closeErr := base.Close(); closeErr != nil {
			_ = derived.Close()
			derived = nil
			base = nil
			return fmt.Errorf("release source snapshot before rendering: %w", closeErr)
		}
		base = nil
		renderSnapshot = derived
	}
	if err := validateEntrypoint(renderSnapshot.FS(), request.Entrypoint); err != nil {
		if derived != nil {
			if closeErr := derived.Close(); closeErr != nil {
				return errors.Join(err, closeErr)
			}
			derived = nil
		} else if base != nil {
			if closeErr := base.Close(); closeErr != nil {
				return errors.Join(err, closeErr)
			}
			base = nil
		}
		return writeFailure(encoder, terminal, "validation", "invalid_request", "entrypoint is not a captured regular file", nil, "")
	}

	renderResult, renderErr := documentcore.RenderSnapshot(ctx, renderSnapshot, renderOptions)
	if renderErr != nil {
		var classified *documentcore.RenderError
		if !errors.As(renderErr, &classified) || classified.Kind != documentcore.ErrorCompile {
			return fmt.Errorf("render snapshot: %w", renderErr)
		}
		terminal.EngineVersion = renderResult.EngineVersion
		terminal.Phase = "compile"
		terminal.FailureCode = "compile_failed"
		terminal.Diagnostics, terminal.RawDiagnostics, terminal.DiagnosticsTruncated = boundedDiagnostics(renderResult.Diagnostics, renderResult.RawOutput)
		if derived != nil {
			outBundle, bundleErr := coreBundle(derived)
			if bundleErr != nil {
				return fmt.Errorf("describe derived snapshot: %w", bundleErr)
			}
			terminal.DerivedContentID = outBundle.Manifest.ID
			if err := renderwire.WriteBundle(encoder, outBundle); err != nil {
				return fmt.Errorf("export derived snapshot: %w", err)
			}
		}
		return writeTerminal(encoder, terminal)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	terminal.Success = true
	terminal.Phase = "complete"
	terminal.EngineVersion = renderResult.EngineVersion
	terminal.Diagnostics, terminal.RawDiagnostics, terminal.DiagnosticsTruncated = boundedDiagnostics(renderResult.Diagnostics, renderResult.RawOutput)
	if derived != nil {
		outBundle, bundleErr := coreBundle(derived)
		if bundleErr != nil {
			return fmt.Errorf("describe derived snapshot: %w", bundleErr)
		}
		terminal.DerivedContentID = outBundle.Manifest.ID
		if err := renderwire.WriteBundle(encoder, outBundle); err != nil {
			return fmt.Errorf("export derived snapshot: %w", err)
		}
	}
	pdf, err := describePDF(ctx, pdfPath)
	if errors.Is(err, errPDFLimit) {
		terminal.Success = false
		terminal.Phase = "resource_limit"
		terminal.FailureCode = "resource_limit"
		terminal.PDF = nil
		return writeTerminal(encoder, terminal)
	}
	if err != nil {
		return fmt.Errorf("inspect rendered PDF: %w", err)
	}
	terminal.PDF = &pdf
	if err := writePDF(encoder, pdfPath, pdf); err != nil {
		return fmt.Errorf("export rendered PDF: %w", err)
	}
	return writeTerminal(encoder, terminal)
}

func workDirectory(name string) (string, error) {
	if name == "" || !filepath.IsAbs(name) {
		return "", errors.New("renderer work directory must be an absolute path")
	}
	info, err := os.Lstat(name)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("renderer work path must be an ordinary directory")
	}
	return filepath.Clean(name), nil
}

func validateRequest(request renderwire.RenderRequest, manifest storagev1.SnapshotManifest) error {
	if !validIdentifier(request.JobID) || !validIdentifier(request.AttemptID) || !validIdentifier(request.SourceSnapshotID) {
		return errors.New("request identifiers are invalid")
	}
	if request.SourceContentID != manifest.ID || !generationv1.LogicalFilePath(request.Entrypoint) {
		return errors.New("request snapshot binding or entrypoint is invalid")
	}
	if request.InputFingerprint == "" {
		return errors.New("request fingerprint is required")
	}
	if request.Execution != nil {
		return validateSelectionRequest(request, manifest)
	}
	job := generationv1.CreateJobRequest{
		Scope: request.Scope, SnapshotID: request.SourceSnapshotID, TargetID: request.TargetID,
		Entrypoint: request.Entrypoint, Parameters: request.Parameters,
		ParametersSchemaPath: request.ParametersSchemaPath,
	}
	encoded, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if len(request.Parameters) > 0 {
		normalized, err := jsonvalue.NormalizeObject(request.Parameters)
		if err != nil || !equalBytes(normalized, request.Parameters) {
			return errors.New("request parameters are not exact normalized JSON")
		}
	}
	_, _, fingerprint, err := generationv1.NormalizeRequest(encoded)
	if err != nil {
		return err
	}
	if fingerprint != request.InputFingerprint {
		return errors.New("request fingerprint does not match request fields")
	}
	return nil
}

func validIdentifier(value string) bool {
	return len(value) > 0 && len(value) <= 256 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func terminalFor(request renderwire.RenderRequest) renderwire.TerminalEnvelope {
	return renderwire.TerminalEnvelope{
		Complete: true, Scope: request.Scope, JobID: request.JobID, AttemptID: request.AttemptID,
		TargetID: request.TargetID, SourceSnapshotID: request.SourceSnapshotID,
		InputFingerprint: request.InputFingerprint, SourceContentID: request.SourceContentID,
		EngineVersion: "0.15.1",
	}
}

func writeFailure(encoder *renderwire.Encoder, terminal renderwire.TerminalEnvelope, phase, failureCode, message string, diagnostics []generationv1.Diagnostic, raw string) error {
	terminal.Success = false
	terminal.Phase = phase
	terminal.FailureCode = failureCode
	terminal.Diagnostics, terminal.RawDiagnostics, terminal.DiagnosticsTruncated = boundedDiagnostics(nil, raw)
	if len(diagnostics) > 0 {
		terminal.Diagnostics = diagnostics
	}
	if message != "" && len(terminal.Diagnostics) == 0 {
		terminal.Diagnostics = []generationv1.Diagnostic{{Severity: "error", Message: message}}
	}
	return writeTerminal(encoder, terminal)
}

func writeTerminal(encoder *renderwire.Encoder, terminal renderwire.TerminalEnvelope) error {
	encoded, err := marshalTerminal(terminal)
	if err != nil {
		return err
	}
	return encoder.WriteBytes(renderwire.Terminal, encoded)
}

func marshalTerminal(terminal renderwire.TerminalEnvelope) ([]byte, error) {
	encoded, err := json.Marshal(terminal)
	if err != nil {
		return nil, err
	}
	if int64(len(encoded)) <= renderwire.EnvelopeLimit {
		return encoded, nil
	}
	terminal.DiagnosticsTruncated = true
	for len(terminal.Diagnostics) > 0 && int64(len(encoded)) > renderwire.EnvelopeLimit {
		terminal.Diagnostics = terminal.Diagnostics[:len(terminal.Diagnostics)-1]
		encoded, err = json.Marshal(terminal)
		if err != nil {
			return nil, err
		}
	}
	for len(terminal.RawDiagnostics) > 0 && int64(len(encoded)) > renderwire.EnvelopeLimit {
		terminal.RawDiagnostics = truncateUTF8(terminal.RawDiagnostics, len(terminal.RawDiagnostics)/2)
		encoded, err = json.Marshal(terminal)
		if err != nil {
			return nil, err
		}
	}
	if int64(len(encoded)) > renderwire.EnvelopeLimit {
		return nil, errors.New("terminal bindings exceed envelope limit")
	}
	return encoded, nil
}

func boundedDiagnostics(input []documentcore.Diagnostic, raw string) ([]generationv1.Diagnostic, string, bool) {
	truncated := !utf8.ValidString(raw) || len(raw) > maxRawDiagnosticSize
	raw = strings.ToValidUTF8(raw, "�")
	raw = truncateUTF8(raw, maxRawDiagnosticSize)
	out := make([]generationv1.Diagnostic, 0, min(len(input), maxDiagnostics))
	if len(input) > maxDiagnostics {
		input = input[:maxDiagnostics]
		truncated = true
	}
	for _, diagnostic := range input {
		converted := generationv1.Diagnostic{
			Severity: safeText(diagnostic.Severity, 64),
			Message:  safeText(diagnostic.Message, maxDiagnosticBytes),
			Raw:      safeText(diagnostic.Raw, maxDiagnosticBytes),
		}
		if diagnostic.Location != nil {
			converted.Path = safeText(diagnostic.Location.Path, 512)
			converted.Line, converted.Column = diagnostic.Location.Line, diagnostic.Location.Column
			if len(diagnostic.Location.Path) > 512 {
				truncated = true
			}
		}
		if len(diagnostic.Hints) > maxDiagnosticList || len(diagnostic.Trace) > maxDiagnosticList {
			truncated = true
		}
		for _, hint := range diagnostic.Hints[:min(len(diagnostic.Hints), maxDiagnosticList)] {
			converted.Hints = append(converted.Hints, safeText(hint, 512))
			if len(hint) > 512 {
				truncated = true
			}
		}
		for _, frame := range diagnostic.Trace[:min(len(diagnostic.Trace), maxDiagnosticList)] {
			converted.Trace = append(converted.Trace, safeText(frame.Message, 512))
			if len(frame.Message) > 512 {
				truncated = true
			}
		}
		if len(diagnostic.Message) > maxDiagnosticBytes || len(diagnostic.Raw) > maxDiagnosticBytes {
			truncated = true
		}
		out = append(out, converted)
	}
	return out, raw, truncated
}

func safeText(value string, limit int) string {
	value = strings.ToValidUTF8(value, "�")
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return '�'
		}
		return r
	}, value)
	return truncateUTF8(value, limit)
}

func truncateUTF8(value string, limit int) string {
	if limit < 0 || len(value) <= limit {
		return value
	}
	value = value[:limit]
	for !utf8.ValidString(value) && len(value) > 0 {
		value = value[:len(value)-1]
	}
	return value
}

func validateEntrypoint(project fs.FS, entrypoint string) error {
	if !generationv1.LogicalFilePath(entrypoint) {
		return fs.ErrInvalid
	}
	info, err := fs.Stat(project, entrypoint)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("entrypoint is not a regular file")
	}
	return nil
}

func toCoreBundle(bundle renderwire.Bundle) documentcore.SnapshotBundle {
	return documentcore.SnapshotBundle{
		Manifest: toCoreManifest(bundle.Manifest), ProjectFS: bundle.ProjectFS,
		ExternalFontFS: bundle.ExternalFontFS,
	}
}

func coreBundle(snapshot *documentcore.Snapshot) (renderwire.Bundle, error) {
	if snapshot == nil {
		return renderwire.Bundle{}, errors.New("snapshot is required")
	}
	coreManifest := snapshot.Manifest()
	manifest := fromCoreManifest(coreManifest)
	if err := storagev1.ValidateManifest(manifest); err != nil {
		return renderwire.Bundle{}, fmt.Errorf("core and storage snapshot identities differ: %w", err)
	}
	external := make(map[int]fs.FS)
	for index, font := range manifest.Fonts {
		if font.InProject {
			continue
		}
		fontFS, err := snapshot.FontFS(index)
		if err != nil {
			return renderwire.Bundle{}, err
		}
		external[index] = fontFS
	}
	return renderwire.Bundle{Manifest: manifest, ProjectFS: snapshot.FS(), ExternalFontFS: external}, nil
}

func toCoreManifest(manifest storagev1.SnapshotManifest) documentcore.SnapshotManifest {
	fonts := make([]documentcore.SnapshotFont, len(manifest.Fonts))
	for i, font := range manifest.Fonts {
		fonts[i] = documentcore.SnapshotFont{Path: font.Path, SourcePath: font.SourcePath, InProject: font.InProject, Files: toCoreFiles(font.Files), Directories: append([]string{}, font.Directories...)}
	}
	return documentcore.SnapshotManifest{
		FormatVersion: manifest.FormatVersion, ID: manifest.ID,
		Origin: documentcore.SourceOrigin{Kind: manifest.Origin.Kind, Location: manifest.Origin.Location, SourceRoot: manifest.Origin.SourceRoot, Ref: manifest.Origin.Ref, Revision: manifest.Origin.Revision, ArchiveSHA256: manifest.Origin.ArchiveSHA256},
		Files:  toCoreFiles(manifest.Files), Directories: append([]string{}, manifest.Directories...), Fonts: fonts,
	}
}

func fromCoreManifest(manifest documentcore.SnapshotManifest) storagev1.SnapshotManifest {
	fonts := make([]storagev1.SnapshotFont, len(manifest.Fonts))
	for i, font := range manifest.Fonts {
		fonts[i] = storagev1.SnapshotFont{Path: font.Path, SourcePath: font.SourcePath, InProject: font.InProject, Files: fromCoreFiles(font.Files), Directories: append([]string{}, font.Directories...)}
	}
	return storagev1.SnapshotManifest{
		FormatVersion: manifest.FormatVersion, ID: manifest.ID,
		Origin: storagev1.SourceOrigin{Kind: manifest.Origin.Kind, Location: manifest.Origin.Location, SourceRoot: manifest.Origin.SourceRoot, Ref: manifest.Origin.Ref, Revision: manifest.Origin.Revision, ArchiveSHA256: manifest.Origin.ArchiveSHA256},
		Files:  fromCoreFiles(manifest.Files), Directories: append([]string{}, manifest.Directories...), Fonts: fonts,
	}
}

func toCoreFiles(files []storagev1.SnapshotFile) []documentcore.SnapshotFile {
	out := make([]documentcore.SnapshotFile, len(files))
	for i, file := range files {
		out[i] = documentcore.SnapshotFile{Path: file.Path, Size: file.Size, SHA256: file.SHA256}
	}
	return out
}

func fromCoreFiles(files []documentcore.SnapshotFile) []storagev1.SnapshotFile {
	out := make([]storagev1.SnapshotFile, len(files))
	for i, file := range files {
		out[i] = storagev1.SnapshotFile{Path: file.Path, Size: file.Size, SHA256: file.SHA256}
	}
	return out
}

var errPDFLimit = errors.New("rendered PDF exceeds the protocol resource limit")

func describePDF(ctx context.Context, name string) (renderwire.PDFDescriptor, error) {
	file, err := os.Open(name)
	if err != nil {
		return renderwire.PDFDescriptor{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return renderwire.PDFDescriptor{}, err
	}
	if !info.Mode().IsRegular() {
		return renderwire.PDFDescriptor{}, errors.New("renderer output is not a regular file")
	}
	if info.Size() > renderwire.PDFLimit {
		return renderwire.PDFDescriptor{}, errPDFLimit
	}
	if info.Size() <= int64(len("%PDF-")) {
		return renderwire.PDFDescriptor{}, errors.New("renderer output is not a nonempty PDF")
	}
	var prefix [5]byte
	if _, err := io.ReadFull(file, prefix[:]); err != nil {
		return renderwire.PDFDescriptor{}, err
	}
	if string(prefix[:]) != "%PDF-" {
		return renderwire.PDFDescriptor{}, errors.New("renderer output does not start with %%PDF-")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return renderwire.PDFDescriptor{}, err
	}
	hash := sha256.New()
	count, err := copyContext(ctx, hash, file)
	if err != nil {
		return renderwire.PDFDescriptor{}, err
	}
	if count != info.Size() {
		return renderwire.PDFDescriptor{}, errors.New("PDF changed while calculating its digest")
	}
	return renderwire.PDFDescriptor{Size: count, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func writePDF(encoder *renderwire.Encoder, name string, descriptor renderwire.PDFDescriptor) error {
	header, err := json.Marshal(descriptor)
	if err != nil {
		return err
	}
	if err := encoder.WriteBytes(renderwire.PDFHeader, header); err != nil {
		return err
	}
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != descriptor.Size {
		return errors.New("PDF changed before export")
	}
	digest, err := hex.DecodeString(descriptor.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return errors.New("invalid PDF digest")
	}
	var sum [sha256.Size]byte
	copy(sum[:], digest)
	return encoder.WriteFrom(renderwire.PDF, descriptor.Size, sum, file)
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

func equalBytes(a, b []byte) bool {
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
