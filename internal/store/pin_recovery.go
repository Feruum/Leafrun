package store

import (
	"context"
	"errors"
)

type UnknownPinAcquire struct {
	JobID   string
	Key     string
	Request []byte
}

// UnknownTerminalPins preserves the durable acquisition key even when its
// response was lost. Cleanup may discover that receipt after authority revoke.
func (s *Store) UnknownTerminalPins(ctx context.Context, limit int) ([]UnknownPinAcquire, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("bounded pin cleanup batch required")
	}
	rows, err := s.pool.Query(ctx, `SELECT p.job_id,p.idempotency_key,p.request FROM generation_pin_acquires p JOIN generation_jobs j ON j.job_id=p.job_id WHERE j.terminal_at IS NOT NULL AND p.response IS NULL AND p.state IN ('pending','failed') ORDER BY p.created_at,p.job_id,p.acquire_number LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UnknownPinAcquire
	for rows.Next() {
		var item UnknownPinAcquire
		if err = rows.Scan(&item.JobID, &item.Key, &item.Request); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
