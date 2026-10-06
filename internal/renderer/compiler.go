package renderer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func entrypointPath(entry string) error {
	if entry == "" || entry == "." || strings.HasPrefix(entry, "-") || path.IsAbs(entry) || filepath.IsAbs(entry) || filepath.VolumeName(entry) != "" || strings.ContainsAny(entry, "\x00\\:") {
		return failure(400, "invalid_entrypoint", "Use a relative Typst file path inside the project.")
	}
	for _, part := range strings.Split(entry, "/") {
		if part == ".." {
			return failure(400, "invalid_entrypoint", "The entrypoint must stay inside the project.")
		}
	}
	return nil
}

func Compile(ctx context.Context, cfg Config, co *Checkout, entry string, data json.RawMessage) (string, error) {
	if err := entrypointPath(entry); err != nil {
		return "", err
	}
	inputPath := filepath.Join(co.ProjectDir, filepath.FromSlash(entry))
	info, err := os.Lstat(inputPath)
	if err != nil || !containsPath(co.ProjectDir, inputPath) || !info.Mode().IsRegular() {
		return "", failure(400, "invalid_entrypoint", "The entrypoint must be an existing regular file inside the project.")
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	input, err := os.CreateTemp(co.ProjectDir, ".render-input-*.json")
	if err != nil {
		return "", err
	}
	if _, err := input.Write(data); err != nil {
		input.Close()
		return "", err
	}
	if err := input.Close(); err != nil {
		return "", err
	}
	packages := filepath.Join(co.RequestDir, "packages")
	localPackages := filepath.Join(co.RequestDir, "local-packages")
	for _, dir := range []string{packages, localPackages} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return "", err
		}
	}
	pdf := filepath.Join(co.RequestDir, "document.pdf")
	args := []string{
		"compile", "--root", co.ProjectDir, "--input", "data_file=/" + filepath.Base(input.Name()),
		"--ignore-system-fonts", "--package-cache-path", packages, "--package-path", localPackages,
		"--jobs", "1", "--diagnostic-format", "short",
	}
	fonts := filepath.Join(co.ProjectDir, "fonts")
	if info, err := os.Stat(fonts); err == nil && info.IsDir() {
		args = append(args, "--font-path", fonts)
	}
	args = append(args, inputPath, pdf)
	_, err = runCommand(ctx, cfg.TypstBin, args, co.ProjectDir, nil)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", err
		}
		apiErr := failure(http.StatusUnprocessableEntity, "typst_error", "Typst could not compile the project.")
		var cmdErr *commandError
		if errors.As(err, &cmdErr) {
			apiErr.Diagnostics = cmdErr.diagnostics
		}
		return "", apiErr
	}
	file, err := os.Open(pdf)
	if err != nil {
		return "", err
	}
	defer file.Close()
	header := make([]byte, 5)
	if n, err := file.Read(header); err != nil || n != len(header) || string(header) != "%PDF-" {
		return "", fmt.Errorf("Typst did not produce a valid PDF header")
	}
	return pdf, nil
}
