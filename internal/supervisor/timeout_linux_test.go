//go:build linux

package supervisor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSupervisorIndependentTimeoutStopsProcessGroup(t *testing.T) {
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
	processIDs := make([]int, 0, 2)
	for _, raw := range strings.Fields(string(pids)) {
		pid, err := strconv.Atoi(raw)
		if err != nil || pid <= 0 {
			t.Fatalf("invalid process id %q in %s: %v", raw, pidFile, err)
		}
		processIDs = append(processIDs, pid)
	}
	if len(processIDs) == 0 {
		t.Fatal("timeout command did not record any process ids")
	}
	t.Cleanup(func() { _ = syscall.Kill(-processIDs[0], syscall.SIGKILL) })
	// SIGKILL is delivered asynchronously. The direct child can be reaped just
	// before the kernel schedules its descendant to exit, so allow a bounded
	// grace while still requiring every member of its process group to stop.
	waitForProcessGroupStopped(t, processIDs[0], time.Second)
}

func waitForProcessGroupStopped(t *testing.T, groupID int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		running, err := runningProcessGroupMembers(groupID)
		if err != nil {
			t.Fatal(err)
		}
		if len(running) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process group members still running after %s: %s", timeout, strings.Join(running, ", "))
		}
		<-ticker.C
	}
}

func runningProcessGroupMembers(groupID int) ([]string, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var running []string
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		data, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return nil, err
		}
		stat := string(data)
		end := strings.LastIndex(stat, ") ")
		if end < 0 {
			return nil, errors.New("malformed process stat for " + strconv.Itoa(pid))
		}
		fields := strings.Fields(stat[end+2:])
		if len(fields) < 3 {
			return nil, errors.New("incomplete process stat for " + strconv.Itoa(pid))
		}
		pgid, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, errors.New("invalid process group in stat for " + strconv.Itoa(pid))
		}
		// A zombie cannot render or retain execution resources. Its reaping is
		// owned by the namespace init, so it is an acceptable terminal state.
		if pgid == groupID && fields[0] != "Z" {
			running = append(running, strconv.Itoa(pid)+"("+fields[0]+")")
		}
	}
	return running, nil
}
