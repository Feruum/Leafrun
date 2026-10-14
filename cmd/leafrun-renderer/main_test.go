package main

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestRendererResourceExitsAreNonretryable(t *testing.T) {
	for _, err := range []error{syscall.ENOSPC, syscall.EDQUOT, fmt.Errorf("copy snapshot: %w", syscall.ENOSPC)} {
		if code := failureExitCode(err); code != 72 {
			t.Fatalf("resource failure exit=%d", code)
		}
	}
	if code := failureExitCode(errors.New("unexpected IO")); code != 1 {
		t.Fatalf("technical failure exit=%d", code)
	}
}
