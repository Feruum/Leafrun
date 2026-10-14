package supervisor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
func TestSupervisorIndependentTimeoutStopsProcessGroup(t *testing.T) {
	requireLinux(t)
	pidFile := filepath.Join(t.TempDir(), "pids")
	started := time.Now()
	err := Run(context.Background(), Options{Command: "/bin/sh", Args: []string{"-c", `echo $$ > "$PID_FILE"; sleep 30 & echo $! >> "$PID_FILE"; wait`}, Env: append(os.Environ(), "PID_FILE="+pidFile), Timeout: time.Second})
	if !errors.Is(err, ErrTimeout) || time.Since(started) > 3*time.Second {
		t.Fatal("timeout did not stop independently", err)
	}
	pids, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range strings.Fields(string(pids)) {
		pid, _ := strconv.Atoi(raw)
		data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		// A killed descendant may briefly remain a zombie until the namespace init
		// reaps it; a zombie cannot render or retain the execution's CPU/memory.
		end := strings.LastIndex(string(data), ") ")
		if end < 0 || data[end+2] != 'Z' {
			t.Fatalf("process %d still running: %s", pid, data)
		}
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
