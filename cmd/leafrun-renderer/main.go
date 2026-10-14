// Command leafrun-renderer runs one generation render over its stdin/stdout
// protocol. The container image supplies /work as a private bounded tmpfs.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Feruum/Leafrun/internal/renderer"
)

const workDir = "/work"

func main() {
	if err := run(); err != nil {
		// Keep stderr independent of request contents, source text, and tool
		// diagnostics. The controller treats any non-zero exit as unpublishable.
		_, _ = fmt.Fprintln(os.Stderr, "renderer execution failed")
		os.Exit(failureExitCode(err))
	}
}

func failureExitCode(err error) int {
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return 72
	}
	return 1
}

func run() error {
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(workDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("renderer work path is not an ordinary directory")
	}
	if err := os.Chdir(workDir); err != nil {
		return err
	}
	if err := setRuntimeEnvironment(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return renderer.Run(ctx, os.Stdin, os.Stdout, renderer.Options{WorkDir: workDir})
}

func setRuntimeEnvironment() error {
	os.Clearenv()
	for key, value := range map[string]string{
		"PATH":   "/usr/local/bin:/usr/bin:/bin",
		"HOME":   workDir,
		"TMPDIR": workDir,
	} {
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	return nil
}
