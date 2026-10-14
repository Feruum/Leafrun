package service

import (
	"context"
	"errors"

	"github.com/Feruum/Leafrun/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RuntimeConfig contains the common dependencies used by the Generation API
// and worker processes. Tokens are accepted only as fixed service credentials;
// caller credentials are resolved separately by Credentials.
type RuntimeConfig struct {
	DatabaseURL        string
	StorageURL         string
	StorageToken       string
	AccessURL          string
	AccessToken        string
	OwnerCallbackToken string
	Credentials        TokenResolver
	EngineVersion      string
}

// Runtime owns the shared PostgreSQL, Storage and Access adapters. The
// exported Store and Server are also the common construction path for later
// controller, janitor and event-relay process roles.
type Runtime struct {
	Pool       *pgxpool.Pool
	Store      *store.Store
	Access     *AccessClient
	Pins       *PinClient
	Server     *Server
	Reconciler *Reconciler
}

func OpenRuntime(ctx context.Context, config RuntimeConfig) (*Runtime, error) {
	pool, state, err := OpenStore(ctx, config.DatabaseURL)
	if err != nil {
		return nil, err
	}
	closeOnError := func(err error) (*Runtime, error) {
		pool.Close()
		return nil, err
	}
	access, err := NewAccessClient(config.AccessURL, config.AccessToken, nil)
	if err != nil {
		return closeOnError(errors.New("Generation Access dependency configuration is invalid"))
	}
	pins, err := NewPinClient(config.StorageURL, config.StorageToken, nil)
	if err != nil {
		return closeOnError(errors.New("Generation Storage dependency configuration is invalid"))
	}
	server, err := New(state, state.Owner(), Config{
		Credentials: config.Credentials, Access: access, Pins: pins,
		OwnerToken: config.OwnerCallbackToken, EngineVersion: config.EngineVersion,
		Ready: func(ctx context.Context) error {
			if err := state.Ready(ctx); err != nil {
				return err
			}
			if err := access.Ready(ctx); err != nil {
				return err
			}
			return pins.Ready(ctx)
		},
	})
	if err != nil {
		return closeOnError(err)
	}
	reconciler, err := NewReconciler(server, 0)
	if err != nil {
		return closeOnError(err)
	}
	return &Runtime{Pool: pool, Store: state, Access: access, Pins: pins, Server: server, Reconciler: reconciler}, nil
}

// OpenStore constructs the database resources shared by standalone roles
// such as migrations and the Temporal dispatcher. The returned pool belongs
// to the caller and must be closed.
func OpenStore(ctx context.Context, databaseURL string) (*pgxpool.Pool, *store.Store, error) {
	if databaseURL == "" {
		return nil, nil, errors.New("Generation database URL is required")
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, nil, errors.New("Generation database URL is invalid")
	}
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, nil, errors.New("Generation database pool could not be created")
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, errors.New("Generation database is unavailable")
	}
	state, err := store.New(pool, store.Config{})
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	return pool, state, nil
}

func (r *Runtime) Close() {
	if r != nil && r.Pool != nil {
		r.Pool.Close()
	}
}
