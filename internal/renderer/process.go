package renderer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const diagnosticLimit = 16 << 10

type cappedBuffer struct {
	bytes.Buffer
	limit     int
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if remain := b.limit - b.Len(); len(p) > remain {
		p = p[:remain]
		b.truncated = true
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}

type commandError struct {
	err         error
	diagnostics string
}

func (e *commandError) Error() string { return e.err.Error() }
func (e *commandError) Unwrap() error { return e.err }

func runCommand(ctx context.Context, executable string, args []string, dir string, extraEnv []string) (string, error) {
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Dir, cmd.Env = dir, append(cleanEnvironment(), extraEnv...)
	configureProcess(cmd)
	cmd.Cancel = func() error { return killProcessTree(cmd) }
	cmd.WaitDelay = 2 * time.Second
	stdout := &cappedBuffer{limit: 8 << 20}
	stderr := &cappedBuffer{limit: diagnosticLimit}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if err != nil {
		return "", &commandError{err: err, diagnostics: strings.TrimSpace(stderr.String())}
	}
	if stdout.truncated {
		return "", fmt.Errorf("subprocess output exceeded the supported limit")
	}
	return strings.TrimSpace(stdout.String()), nil
}

func cleanEnvironment() []string {
	allowed := map[string]bool{
		"PATH": true, "SYSTEMROOT": true, "WINDIR": true,
		"TEMP": true, "TMP": true, "TMPDIR": true,
		"LANG": true, "LC_ALL": true, "SSL_CERT_FILE": true, "SSL_CERT_DIR": true,
	}
	var env []string
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if allowed[strings.ToUpper(key)] {
			env = append(env, value)
		}
	}
	return env
}

func gitError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	e := failure(502, "git_error", "The Git repository could not be fetched or prepared.")
	var commandErr *commandError
	if errors.As(err, &commandErr) {
		e.Diagnostics = commandErr.diagnostics
	}
	return e
}
