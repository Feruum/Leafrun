// Package store owns Generation's durable state. Every clock-sensitive
// transition is decided by PostgreSQL, and external effects are represented by
// immutable intents before their caller performs them.
package store

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/leafrun/leafrun-platform/events"
)

//go:embed migrations/*.sql
var migrations embed.FS

const (
	queueLockID     int64 = 0x47454e5155455545
	cleanupLockID   int64 = 0x47454e434c45414e
	defaultAttempts       = 3
)

var (
	ErrConflict          = errors.New("generation state conflicts with an existing immutable binding")
	ErrCapacity          = errors.New("generation capacity is full")
	ErrNotFound          = errors.New("generation job or record not found")
	ErrInvalidTransition = errors.New("generation state transition is not allowed")
	ErrLeaseLost         = errors.New("generation lease was lost or expired")
	ErrPublicationHeld   = errors.New("publication lease is held by another publisher")
	ErrCandidateExpired  = errors.New("publication candidate has expired")
	ErrReceiptExpired    = errors.New("idempotency receipt has expired")
)

// Config accepts lower test limits while placing production bounds on every
// configurable quota/lease. Zero values select the E04 defaults.
type Config struct {
	QueueGlobal        int
	QueueWorkspace     int
	ExecutionGlobal    int
	ExecutionWorkspace int
	AttemptLease       time.Duration
	PublicationLease   time.Duration
	ReceiptTTL         time.Duration
	DiagnosticTTL      time.Duration
	HistoryTTL         time.Duration
	PinHolderID        string
	PinHolderDomain    string
}

func (c Config) withDefaults() (Config, error) {
	if c.QueueGlobal == 0 {
		c.QueueGlobal = 50
	}
	if c.QueueWorkspace == 0 {
		c.QueueWorkspace = 10
	}
	if c.ExecutionGlobal == 0 {
		c.ExecutionGlobal = 2
	}
	if c.ExecutionWorkspace == 0 {
		c.ExecutionWorkspace = 1
	}
	if c.AttemptLease == 0 {
		c.AttemptLease = 30 * time.Second
	}
	if c.PublicationLease == 0 {
		c.PublicationLease = 30 * time.Second
	}
	if c.ReceiptTTL == 0 {
		c.ReceiptTTL = 7 * 24 * time.Hour
	}
	if c.DiagnosticTTL == 0 {
		c.DiagnosticTTL = 24 * time.Hour
	}
	if c.HistoryTTL == 0 {
		c.HistoryTTL = 30 * 24 * time.Hour
	}
	if c.PinHolderID == "" {
		c.PinHolderID = "leafrun-generation"
	}
	if c.PinHolderDomain == "" {
		c.PinHolderDomain = "service"
	}
	if c.QueueGlobal < 1 || c.QueueGlobal > 50 || c.QueueWorkspace < 1 || c.QueueWorkspace > 10 ||
		c.ExecutionGlobal < 1 || c.ExecutionGlobal > 2 || c.ExecutionWorkspace < 1 || c.ExecutionWorkspace > 1 {
		return Config{}, errors.New("generation quotas must be within the E04 bounded defaults")
	}
	if c.AttemptLease < 3*time.Second || c.AttemptLease > 30*time.Second || c.PublicationLease < 3*time.Second || c.PublicationLease > 30*time.Second {
		return Config{}, errors.New("generation leases must be between 3 and 30 seconds")
	}
	if c.ReceiptTTL <= 0 || c.ReceiptTTL > 7*24*time.Hour || c.DiagnosticTTL <= 0 || c.DiagnosticTTL > 24*time.Hour || c.HistoryTTL <= 0 || c.HistoryTTL > 30*24*time.Hour {
		return Config{}, errors.New("generation retention durations exceed the E04 bounds")
	}
	return c, nil
}

type execer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// Store uses one Generation database. The same database transaction also
// records Platform outbox events.
type Store struct {
	pool *pgxpool.Pool
	cfg  Config
}

func New(pool *pgxpool.Pool, config Config) (*Store, error) {
	if pool == nil {
		return nil, errors.New("generation PostgreSQL pool is required")
	}
	cfg, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool, cfg: cfg}, nil
}

// Migrate applies the shared outbox schema and Generation-owned schema.
func (s *Store) Migrate(ctx context.Context) error {
	if err := events.ApplyMigration(ctx, s.pool); err != nil {
		return err
	}
	sql, err := fs.ReadFile(migrations, "migrations/001_generation.sql")
	if err != nil {
		return fmt.Errorf("read generation migration: %w", err)
	}
	if _, err := s.pool.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("apply generation migration: %w", err)
	}
	return nil
}

func newID(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate %s id: %w", prefix, err)
	}
	return prefix + hex.EncodeToString(b[:]), nil
}

func begin(ctx context.Context, db interface {
	Begin(context.Context) (pgx.Tx, error)
}) (pgx.Tx, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin generation transaction: %w", err)
	}
	return tx, nil
}

func lockQueue(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, queueLockID); err != nil {
		return fmt.Errorf("lock generation quotas: %w", err)
	}
	return nil
}

func lockCleanup(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, cleanupLockID); err != nil {
		return fmt.Errorf("lock generation cleanup: %w", err)
	}
	return nil
}

func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit generation transaction: %w", err)
	}
	return nil
}

func rollback(ctx context.Context, tx pgx.Tx) { _ = tx.Rollback(ctx) }
