//go:build linux

package supervisor

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// Run starts a fresh process group and streams its pipes unchanged. On timeout
// or cancellation it kills the group and direct child, then bounds the reaping
// wait so PID 1 can exit if the child's status remains unavailable.
func Run(ctx context.Context, options Options) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	if timeout < 0 || timeout > 60*time.Second || options.Command == "" {
		return errors.New("invalid supervisor command or attempt timeout")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	cmd := exec.Command(options.Command, options.Args...)
	cmd.Env = options.Env
	cmd.Stdin = options.Stdin
	cmd.Stdout = options.Stdout
	if cmd.Stdout == nil {
		cmd.Stdout = io.Discard
	}
	cmd.Stderr = options.Stderr
	if cmd.Stderr == nil {
		cmd.Stderr = io.Discard
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		return err
	}
	stopped := make(chan error, 1)
	go func() { stopped <- cmd.Wait() }()
	var result error
	select {
	case result = <-stopped:
		result = errors.Join(result, killGroup(cmd.Process.Pid))
	case <-timer.C:
		killErr := killGroupAndChild(cmd.Process)
		result = errors.Join(ErrTimeout, killErr, waitAfterKill(stopped, reapWaitLimit))
	case <-ctx.Done():
		killErr := killGroupAndChild(cmd.Process)
		result = errors.Join(ctx.Err(), killErr, waitAfterKill(stopped, reapWaitLimit))
	}
	if os.Getpid() == 1 {
		for {
			var status syscall.WaitStatus
			pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
			if pid <= 0 || err != nil {
				break
			}
		}
	}
	return result
}

const reapWaitLimit = 2 * time.Second

func killGroupAndChild(process *os.Process) error {
	if process == nil {
		return errors.New("supervisor child process is missing")
	}
	return errors.Join(killGroup(process.Pid), killProcess(process))
}

func killProcess(process *os.Process) error {
	if err := process.Kill(); errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
		return nil
	} else {
		return err
	}
}

func waitAfterKill(stopped <-chan error, limit time.Duration) error {
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case <-stopped:
		return nil
	case <-timer.C:
		return ErrReapTimeout
	}
}

func killGroup(pid int) error {
	err := syscall.Kill(-pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
