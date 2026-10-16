package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/leafrun/leafrun-platform/events"
)

var generationMigrationFiles = []string{"migrations/001_generation.sql", "migrations/002_e05_render_selection.sql", "migrations/003_e06_authority.sql", "migrations/004_e06_audit.sql"}

type migrationQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func generationChecksum(i int) string {
	b, _ := migrations.ReadFile(generationMigrationFiles[i])
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func generationLedger(ctx context.Context, q migrationQuerier) (int, error) {
	rows, err := q.Query(ctx, `SELECT version,checksum FROM generation_schema_migrations ORDER BY version`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var version int
		var sum string
		if rows.Scan(&version, &sum) != nil || n >= len(generationMigrationFiles) || version != n+1 || sum != generationChecksum(n) {
			return 0, errors.New("Generation migration ledger mismatch")
		}
		n++
	}
	return n, rows.Err()
}
func (s *Store) Migrate(ctx context.Context) error {
	if err := events.ApplyMigration(ctx, s.pool); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(164739208605); CREATE TABLE IF NOT EXISTS generation_schema_migrations(version integer PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return err
	}
	n, err := generationLedger(ctx, tx)
	if err != nil {
		return err
	}
	for i := n; i < len(generationMigrationFiles); i++ {
		sql, readErr := migrations.ReadFile(generationMigrationFiles[i])
		if readErr != nil {
			return readErr
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO generation_schema_migrations(version,checksum)VALUES($1,$2)`, i+1, generationChecksum(i)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
