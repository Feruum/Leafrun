package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Feruum/Leafrun/internal/sandbox"
	"github.com/Feruum/Leafrun/internal/store"
	generation "github.com/leafrun/leafrun-contracts/generation/v1"
	identity "github.com/leafrun/leafrun-contracts/identity/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

type createWaitEngine struct {
	*runtimeEngine
	entered chan struct{}
	resume  <-chan struct{}
	starts  atomic.Int32
}

func TestPostgresExecutionStartRechecksLeaseAfterAuthorityWait(t *testing.T) {
	state, pool := privateStore(t)
	job := queuedRuntimeJob(t, state)
	attempt, err := state.ClaimExecution(context.Background(), store.ClaimAttemptInput{JobID: job.JobID, CreateContainerIntentKey: "start-clock", StagingToken: "start-clock", PrivateStagingPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	state.SetAuthorityGate(func(context.Context, store.AuthorityBinding, string) error {
		_, e := pool.Exec(context.Background(), `UPDATE generation_attempts SET owner_lease_until=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, attempt.AttemptID)
		return e
	})
	if err = state.CheckExecutionStart(context.Background(), job.JobID, attempt.AttemptID, attempt.FencingEpoch); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expired final start lease accepted: %v", err)
	}
}

func (e *createWaitEngine) Create(ctx context.Context, spec sandbox.Spec) (string, error) {
	close(e.entered)
	select {
	case <-e.resume:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return e.runtimeEngine.Create(ctx, spec)
}
func (e *createWaitEngine) Start(ctx context.Context, id string) error {
	e.starts.Add(1)
	return e.runtimeEngine.Start(ctx, id)
}

func TestPostgresExecutionStartRechecksSavedPurposeAfterCreateWait(t *testing.T) {
	for _, purpose := range []string{"working-source", "published-release"} {
		for _, deny := range []bool{true, false} {
			name := "allowed"
			if deny {
				name = "revoked"
			}
			t.Run(purpose+"/"+name, func(t *testing.T) {
				state, pool := privateStore(t)
				job := queuedRuntimeJob(t, state)
				original := identity.Identity{Principal: identity.Principal{Subject: "runtime-actor", Domain: "user"}, Authority: identity.AuthorityRef{Kind: identity.KindKratosSession, ID: "original-start-session", Version: 1}}
				raw, _ := json.Marshal(original)
				release, digest := "", ""
				if purpose == "published-release" {
					release = "frozen-release"
					digest = strings.Repeat("d", 64)
				}
				if _, err := pool.Exec(context.Background(), `UPDATE generation_jobs SET original_identity=$2,authority_release_id=$3,authority_descriptor_digest=$4 WHERE job_id=$1`, job.JobID, raw, release, digest); err != nil {
					t.Fatal(err)
				}
				var revoked atomic.Bool
				var gates atomic.Int32
				// Models the already-installed purpose-aware owner gate: working source.read
				// or exact published release binding changes, while generation.execute stays
				// allowed. Real PostgreSQL admission, attempt, intents and cleanup are used.
				state.SetAuthorityGate(func(ctx context.Context, b store.AuthorityBinding, op string) error {
					gates.Add(1)
					current, ok := authn.IdentityFromContext(ctx)
					if !ok || current != original || b.Identity == nil || *b.Identity != original || b.JobID != job.JobID || b.Scope != job.Scope || b.SourceSnapshotID != job.SourceSnapshotID || b.ReleaseID != release || b.DescriptorDigest != digest || op != "generation.execute" && op != "generation.publish" {
						t.Error("saved purpose/original authority binding changed")
						return store.ErrAuthorityDenied
					}
					if revoked.Load() {
						return store.ErrAuthorityDenied
					}
					return nil
				})
				resume := make(chan struct{})
				engine := &createWaitEngine{runtimeEngine: newRuntimeEngine(job, sandbox.State{ID: strings.Repeat("a", 64), Status: "exited", ExitCode: 0}), entered: make(chan struct{}), resume: resume}
				controller, err := New(state, newRuntimeStorage(job), engine, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: t.TempDir(), OwnerID: "start-owner", RendererImage: runtimeImageID, Heartbeat: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- controller.Execute(ctx, job.JobID) }()
				select {
				case <-engine.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("normal Docker Create was not reached")
				}
				revoked.Store(deny)
				close(resume)
				select {
				case <-done:
				case <-time.After(8 * time.Second):
					t.Fatal("execution did not finish")
				}
				current, err := state.GetJob(context.Background(), job.JobID)
				if err != nil {
					t.Fatal(err)
				}
				if deny {
					if engine.starts.Load() != 0 {
						t.Fatal("Docker Start ran after saved source/release purpose was revoked during Create")
					}
					if current.Status != generation.Failed || current.FailureCode != "access_denied" || !engine.removed {
						t.Fatal("denied start did not durably fail and clean the created container")
					}
				} else if engine.starts.Load() != 1 || current.Status != generation.Succeeded {
					t.Fatalf("authorized start failed: %s %s", current.Status, current.FailureCode)
				}
				if gates.Load() < 2 {
					t.Fatal("saved purpose was not rechecked after Create")
				}
				var reservations int
				if pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_reservations WHERE job_id=$1`, job.JobID).Scan(&reservations) != nil || reservations != 0 {
					t.Fatal("completed start boundary retained quota")
				}
			})
		}
	}
}
