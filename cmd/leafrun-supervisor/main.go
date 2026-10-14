// Command leafrun-supervisor is PID 1 in each isolated Linux renderer container.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/Feruum/Leafrun/internal/supervisor"
)

func main() {
	timeout := flag.Duration("timeout", 60*time.Second, "independent attempt timeout (maximum 60s)")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		args = []string{"/leafrun-renderer"}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := supervisor.Run(ctx, supervisor.Options{Command: args[0], Args: args[1:], Env: os.Environ(), Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr, Timeout: *timeout})
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err)
	if errors.Is(err, supervisor.ErrTimeout) {
		os.Exit(124)
	}
	if errors.Is(err, context.Canceled) {
		os.Exit(130)
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		code := exit.ExitCode()
		if code < 0 {
			code = 137
		}
		os.Exit(code)
	}
	os.Exit(125)
}
