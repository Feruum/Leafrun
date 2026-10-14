//go:build linux

package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

const (
	escapeHelperEnv = "LEAFRUN_SUPERVISOR_ESCAPE_HELPER"
	memberHelperEnv = "LEAFRUN_SUPERVISOR_MEMBER_HELPER"
	groupPathEnv    = "LEAFRUN_SUPERVISOR_GROUP_PATH"
	readyPathEnv    = "LEAFRUN_SUPERVISOR_READY_PATH"
)

func TestSupervisorTimeoutKillsDirectChildAfterItChangesProcessGroup(t *testing.T) {
	readyPath := filepath.Join(t.TempDir(), "group-ready")
	groupPath := filepath.Join(filepath.Dir(readyPath), "group-id")
	defer killTestGroup(groupPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	started := time.Now()
	go func() {
		runDone <- Run(ctx, Options{
			Command: os.Args[0],
			Args:    []string{"-test.run=^TestSupervisorProcessGroupEscapeHelper$"},
			Env:     append(os.Environ(), escapeHelperEnv+"=1", groupPathEnv+"="+groupPath, readyPathEnv+"="+readyPath),
			Timeout: 2 * time.Second,
		})
	}()

	groupID, err := waitForGroupReady(readyPath, runDone, 4*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// The helper direct child and its descendant moved into this group. The
	// original supervisor group kill cannot reach either process after that.
	defer func() { _ = syscall.Kill(-groupID, syscall.SIGKILL) }()

	select {
	case err := <-runDone:
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("Run() error = %v, want timeout", err)
		}
		if elapsed := time.Since(started); elapsed > 4*time.Second {
			t.Fatalf("Run() took %s to reap escaped direct child", elapsed)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timeout did not return after killing the direct child")
	}
}

func TestSupervisorCancellationKillsDirectChildAfterItChangesProcessGroup(t *testing.T) {
	readyPath := filepath.Join(t.TempDir(), "group-ready")
	groupPath := filepath.Join(filepath.Dir(readyPath), "group-id")
	defer killTestGroup(groupPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- Run(ctx, Options{
			Command: os.Args[0],
			Args:    []string{"-test.run=^TestSupervisorProcessGroupEscapeHelper$"},
			Env:     append(os.Environ(), escapeHelperEnv+"=1", groupPathEnv+"="+groupPath, readyPathEnv+"="+readyPath),
			Timeout: 20 * time.Second,
		})
	}()
	groupID, err := waitForGroupReady(readyPath, runDone, 4*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-groupID, syscall.SIGKILL) }()
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context cancellation", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("cancellation did not return after killing the direct child")
	}
}

// TestSupervisorProcessGroupEscapeHelper runs as the supervisor's direct child.
// It starts a group leader, joins that group's process group, then holds open
// until the test cleans it up or the fallback timer fires.
func TestSupervisorProcessGroupEscapeHelper(t *testing.T) {
	if os.Getenv(escapeHelperEnv) != "1" {
		return
	}
	member := exec.Command(os.Args[0], "-test.run=^TestSupervisorProcessGroupMemberHelper$")
	member.Env = append(os.Environ(), memberHelperEnv+"=1")
	member.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := member.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start group member:", err)
		os.Exit(121)
	}
	groupID := member.Process.Pid
	groupPath := os.Getenv(groupPathEnv)
	if err := os.WriteFile(groupPath, []byte(strconv.Itoa(groupID)), 0o600); err != nil {
		_ = syscall.Kill(-groupID, syscall.SIGKILL)
		fmt.Fprintln(os.Stderr, "write group marker:", err)
		os.Exit(124)
	}
	if err := syscall.Setpgid(0, groupID); err != nil {
		_ = member.Process.Kill()
		_ = member.Wait()
		fmt.Fprintln(os.Stderr, "join child group:", err)
		os.Exit(122)
	}
	if syscall.Getpgrp() != groupID {
		_ = syscall.Kill(-groupID, syscall.SIGKILL)
		fmt.Fprintln(os.Stderr, "direct child did not join new group")
		os.Exit(123)
	}
	readyPath := os.Getenv(readyPathEnv)
	if err := os.WriteFile(readyPath, []byte(strconv.Itoa(groupID)), 0o600); err != nil {
		_ = syscall.Kill(-groupID, syscall.SIGKILL)
		fmt.Fprintln(os.Stderr, "write ready marker:", err)
		os.Exit(125)
	}
	time.Sleep(8 * time.Second)
	_ = syscall.Kill(-groupID, syscall.SIGKILL)
	_ = member.Wait()
	os.Exit(0)
}

func TestSupervisorProcessGroupMemberHelper(t *testing.T) {
	if os.Getenv(memberHelperEnv) != "1" {
		return
	}
	time.Sleep(12 * time.Second)
}

func TestSupervisorWaitAfterKillIsBounded(t *testing.T) {
	started := time.Now()
	err := waitAfterKill(make(chan error), 25*time.Millisecond)
	if !errors.Is(err, ErrReapTimeout) {
		t.Fatalf("waitAfterKill() error = %v, want ErrReapTimeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded wait took %s", elapsed)
	}
}

func killTestGroup(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	groupID, err := strconv.Atoi(string(data))
	if err == nil && groupID > 0 {
		_ = syscall.Kill(-groupID, syscall.SIGKILL)
	}
}

func waitForGroupReady(path string, runDone <-chan error, timeout time.Duration) (int, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			groupID, parseErr := strconv.Atoi(string(data))
			if parseErr != nil || groupID <= 0 {
				return 0, fmt.Errorf("invalid helper process group id %q: %v", data, parseErr)
			}
			return groupID, nil
		}
		if !os.IsNotExist(err) {
			return 0, err
		}
		select {
		case err := <-runDone:
			return 0, fmt.Errorf("supervisor exited before helper changed groups: %w", err)
		case <-timer.C:
			return 0, errors.New("helper did not establish its new process group in time")
		case <-ticker.C:
		}
	}
}
