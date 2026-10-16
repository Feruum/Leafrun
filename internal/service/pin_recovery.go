package service

import (
	"context"
	"errors"
	"time"

	"github.com/Feruum/Leafrun/internal/store"
	"github.com/leafrun/leafrun-platform/authn"
)

type unknownPinRecoveryStore interface {
	UnknownTerminalPins(context.Context, int) ([]store.UnknownPinAcquire, error)
}

func (s *Server) recoverUnknownPins(ctx context.Context) error {
	state, ok := s.store.(unknownPinRecoveryStore)
	if !ok {
		return nil
	}
	ctx = authn.WithoutIdentity(ctx)
	items, err := state.UnknownTerminalPins(ctx, 100)
	if err != nil {
		return err
	}
	var failures []error
	for _, item := range items {
		request, decodeErr := decodePinRequest(item.Request)
		if decodeErr != nil || request.IdempotencyKey != item.Key {
			failures = append(failures, errors.New("invalid durable cleanup pin request"))
			continue
		}
		job, getErr := s.store.GetJob(ctx, item.JobID)
		if getErr != nil || !job.Status.Terminal() || request.Scope != job.Scope {
			failures = append(failures, errors.New("cleanup pin/job binding unavailable"))
			continue
		}
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		pin, acquireErr := s.cfg.Pins.AcquirePin(call, request)
		cancel()
		if isExpiredPinError(acquireErr) {
			acquireErr = s.store.RecordPinAcquireExpired(ctx, item.JobID, item.Key)
		} else if acquireErr == nil {
			if acquireErr = validatePin(request, pin); acquireErr == nil {
				acquireErr = s.store.RecordPinAcquireResponse(ctx, item.JobID, item.Key, pin)
			}
		}
		if acquireErr != nil {
			failures = append(failures, errors.New("unknown terminal pin cleanup is pending"))
		}
	}
	return errors.Join(failures...)
}
