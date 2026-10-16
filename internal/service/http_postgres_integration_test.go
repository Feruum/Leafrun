package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Feruum/Leafrun/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func openHTTPStoreTest(t *testing.T) *store.Store {
	state, _ := openHTTPStoreTestWithPool(t)
	return state
}

func openHTTPStoreTestWithPool(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("GENERATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("GENERATION_TEST_DATABASE_URL is not set")
	}
	adminConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil || adminConfig.ConnConfig.Database != "generation_test" {
		t.Fatal("Generation HTTP tests require the dedicated generation_test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to isolated Generation test database: %v", err)
	}
	if err = admin.Ping(ctx); err != nil {
		admin.Close()
		t.Fatalf("ping isolated Generation test database: %v", err)
	}
	var random [12]byte
	if _, err = rand.Read(random[:]); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	schema := "generation_http_" + hex.EncodeToString(random[:])
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatalf("create private test schema: %v", err)
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatal(err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatalf("connect to private test schema: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dropCancel()
		_, _ = admin.Exec(dropCtx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	})
	state, err := store.New(pool, store.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.Migrate(ctx); err != nil {
		t.Fatalf("apply Generation migrations: %v", err)
	}
	return state, pool
}

func TestPostgresHTTPAdmissionPreflightsAndPreservesReceiptAfterSourceExpiry(t *testing.T) {
	state := openHTTPStoreTest(t)
	access := &testAccess{allowed: true}
	pins := &testPins{}
	configureTestSnapshot(t, pins)
	handler, err := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true,
		Credentials: StaticTokens{"valid-token": {Subject: "user-1", Domain: "core"}},
		Access:      access, Pins: pins, OwnerToken: "owner-secret", EngineVersion: "0.15.1",
		Ready: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	first := createRequest(handler, createBody, "postgres-http-receipt")
	if first.Code != 202 {
		t.Fatalf("create status = %d, body %s", first.Code, first.Body)
	}
	var admission generationv1.Admission
	if err = json.Unmarshal(first.Body.Bytes(), &admission); err != nil {
		t.Fatal(err)
	}
	if active, activateErr := state.AdmissionActivated(context.Background(), admission.Job.JobID); activateErr != nil || !active {
		t.Fatalf("admission not active: %v, %v", active, activateErr)
	}

	pins.mu.Lock()
	pins.snapshotErr = &HTTPStatusError{StatusCode: 410}
	firstSnapshotReads := pins.snapshotCalls
	pins.mu.Unlock()
	replay := createRequest(handler, createBody, "postgres-http-receipt")
	if replay.Code != 202 {
		t.Fatalf("replay after source expiry status = %d, body %s", replay.Code, replay.Body)
	}
	pins.mu.Lock()
	if pins.snapshotCalls != firstSnapshotReads {
		pins.mu.Unlock()
		t.Fatal("valid receipt replay performed a source snapshot read")
	}
	pins.mu.Unlock()

	missing := createRequest(handler, createBody, "postgres-http-missing-source")
	if missing.Code != 404 {
		t.Fatalf("fresh missing snapshot status = %d, body %s", missing.Code, missing.Body)
	}
	initiator, err := EncodeInitiator(storagev1.Principal{Subject: "user-1", Domain: "core"})
	if err != nil {
		t.Fatal(err)
	}
	request, _, fingerprint, err := generationv1.NormalizeRequest([]byte(createBody))
	if err != nil {
		t.Fatal(err)
	}
	if _, found, lookupErr := state.LookupAdmission(context.Background(), request.Scope.WorkspaceID, initiator, "postgres-http-missing-source", fingerprint); lookupErr != nil || found {
		t.Fatalf("missing source created a receipt: found=%v err=%v", found, lookupErr)
	}
}

func TestPostgresHTTPEntrypointFailureDoesNotReserveQueue(t *testing.T) {
	state := openHTTPStoreTest(t)
	pins := &testPins{}
	configureTestSnapshot(t, pins)
	handler, err := New(state, state.Owner(), Config{AuthMode: "dev", DevFixtures: true,
		Credentials: StaticTokens{"valid-token": {Subject: "user-1", Domain: "core"}},
		Access:      &testAccess{allowed: true}, Pins: pins, OwnerToken: "owner-secret", EngineVersion: "0.15.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"scope":{"workspaceId":"ws-1","projectId":"project-1"},"snapshotId":"snap-1","targetId":"pdf","entrypoint":"absent.typ"}`
	response := createRequest(handler, body, "postgres-http-missing-entrypoint")
	if response.Code != 422 {
		t.Fatalf("missing entrypoint status = %d, body %s", response.Code, response.Body)
	}
	initiator, _ := EncodeInitiator(storagev1.Principal{Subject: "user-1", Domain: "core"})
	request, _, fingerprint, err := generationv1.NormalizeRequest([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, found, lookupErr := state.LookupAdmission(context.Background(), request.Scope.WorkspaceID, initiator, "postgres-http-missing-entrypoint", fingerprint); lookupErr != nil || found {
		t.Fatalf("missing entrypoint created a receipt: found=%v err=%v", found, lookupErr)
	}
}
