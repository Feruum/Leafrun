package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Reconciler repairs admissions that were durably saved before a Storage or
// database acknowledgement was lost, and runs the bounded expiry/retention
// transitions owned by the Store.
type Reconciler struct {
	server   *Server
	interval time.Duration
}

func NewReconciler(server *Server, interval time.Duration) (*Reconciler, error) {
	if server == nil {
		return nil, errors.New("Generation server is required")
	}
	if interval == 0 {
		interval = 5 * time.Second
	}
	if interval < time.Second || interval > time.Minute {
		return nil, errors.New("reconciler interval must be between one second and one minute")
	}
	return &Reconciler{server: server, interval: interval}, nil
}

func (r *Reconciler) RunOnce(ctx context.Context) error {
	cleanupErr := r.server.recoverUnknownPins(ctx)
	ids, err := r.server.store.PendingAdmissions(ctx, 100)
	if err != nil {
		return fmt.Errorf("list pending Generation admissions: %w", err)
	}
	var failures []error
	if cleanupErr != nil {
		failures = append(failures, cleanupErr)
	}
	for _, jobID := range ids {
		job, getErr := r.server.store.GetJob(ctx, jobID)
		if getErr == nil {
			getErr = r.server.EnsureSourcePin(ctx, job)
		}
		if getErr != nil {
			failures = append(failures, fmt.Errorf("recover Generation admission %s: %w", jobID, getErr))
		}
	}
	if _, err = r.server.store.ExpireDue(ctx, 100); err != nil {
		failures = append(failures, fmt.Errorf("expire due Generation jobs: %w", err))
	}
	if _, err = r.server.store.Retain(ctx); err != nil {
		failures = append(failures, fmt.Errorf("retain Generation history: %w", err))
	}
	return errors.Join(failures...)
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Dependency errors may be temporary. Keep retrying on the bounded
			// interval; the public log intentionally omits job and source data.
			slog.Error("Generation reconciliation iteration failed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
