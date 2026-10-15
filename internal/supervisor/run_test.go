package supervisor

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func requireLinux(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("Linux process groups are verified in the Linux qualification job")
	}
}
func TestSupervisorStreamsBinaryAndRetainsExitCode(t *testing.T) {
	requireLinux(t)
	var out bytes.Buffer
	if err := Run(context.Background(), Options{Command: "/bin/sh", Args: []string{"-c", `printf '\000\377\200x'`}, Timeout: time.Second, Stdout: &out}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), []byte{0, 255, 128, 'x'}) {
		t.Fatalf("changed binary stream: %v", out.Bytes())
	}
	err := Run(context.Background(), Options{Command: "/bin/sh", Args: []string{"-c", "exit 7"}, Timeout: time.Second})
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 7 {
		t.Fatal("lost exit code", err)
	}
}
func TestSupervisorCancellationAndLimitValidation(t *testing.T) {
	requireLinux(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, Options{Command: "/bin/sh", Args: []string{"-c", "sleep 30"}, Timeout: time.Second}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := Run(context.Background(), Options{Command: "/bin/sh", Args: []string{"-c", "exit 0"}, Timeout: 61 * time.Second}); err == nil {
		t.Fatal("accepted an unbounded attempt timeout")
	}
}
