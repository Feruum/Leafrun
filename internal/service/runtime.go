package service

import (
	"context"
	"errors"

	"github.com/Feruum/Leafrun/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

// RuntimeConfig contains the common dependencies used by the Generation API
// and worker processes. Tokens are accepted only as fixed service credentials;
// caller credentials are resolved separately by Credentials.
type RuntimeConfig struct {
	AuditCleanupAuthority identityv1.AuthorityRef
	AuthMode              string
	DevFixtures           bool
	GuestCookieName       string
	CookieName            string
	BrowserOrigins        []string
	LoopbackHTTP          bool
	DatabaseURL           string
	StorageURL            string
	StorageToken          string
	AccessURL             string
	AccessToken           string
	ProjectsURL           string
	ProjectsToken         string
	OwnerCallbackToken    string
	Credentials           TokenResolver
	EngineVersion         string
}

// Runtime owns the shared PostgreSQL, Storage and Access adapters. The
// exported Store and Server are also the common construction path for later
// controller, janitor and event-relay process roles.
type Runtime struct {
	Pool       *pgxpool.Pool
	Store      *store.Store
	Access     *AccessClient
	Pins       *PinClient
	Projects   *projectsv1.Client
	Server     *Server
	Reconciler *Reconciler
}

func OpenRuntime(ctx context.Context, config RuntimeConfig) (*Runtime, error) {
	if config.AuthMode == "access" && (config.AuditCleanupAuthority.Validate() != nil || config.AuditCleanupAuthority.Kind != identityv1.KindService) {
		return nil, errors.New("explicit Generation audit cleanup authority required")
	}
	if err := config.ValidateCredentialSeparation(); err != nil {
		return nil, err
	}
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
	var authenticator authn.Authenticator
	var browserGuard *authn.BrowserGuard
	if config.AuthMode == "access" {
		public, createErr := authn.NewClientWithCookieNames(config.AccessURL, config.AccessToken, authn.ConfiguredCookieNames(config.CookieName, config.GuestCookieName), nil)
		if createErr != nil {
			return closeOnError(createErr)
		}
		authenticator = public
		access.current = public
		browserGuard, err = authn.NewBrowserGuard(config.BrowserOrigins, config.LoopbackHTTP)
		if err != nil {
			return closeOnError(err)
		}
	}
	pins, err := NewPinClient(config.StorageURL, config.StorageToken, nil)
	if err != nil {
		return closeOnError(errors.New("Generation Storage dependency configuration is invalid"))
	}
	var projects *projectsv1.Client
	if (config.ProjectsURL == "") != (config.ProjectsToken == "") {
		return closeOnError(errors.New("Generation Projects URL and token must be configured together"))
	}
	if config.ProjectsURL != "" {
		projects, err = projectsv1.NewClient(config.ProjectsURL, config.ProjectsToken, nil)
		if err != nil {
			return closeOnError(errors.New("Generation Projects dependency configuration is invalid"))
		}
	}
	server, err := New(state, state.Owner(), Config{
		AuditCleanupAuthority: config.AuditCleanupAuthority,
		AuthMode:              config.AuthMode, DevFixtures: config.DevFixtures, Authenticator: authenticator, CookieName: config.CookieName, GuestCookieName: config.GuestCookieName, BrowserGuard: browserGuard, InternalTokens: []string{config.StorageToken, config.AccessToken, config.ProjectsToken},
		Credentials: config.Credentials, Access: access, Pins: pins, Resources: pins, Projects: projects,
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
	if config.AuthMode == "access" {
		state.SetAuthorityGate(server.durableAuthorityGate)
		state.SetRequestAuthorityGate(server.requestAuthorityGate)
	}
	reconciler, err := NewReconciler(server, 0)
	if err != nil {
		return closeOnError(err)
	}
	return &Runtime{Pool: pool, Store: state, Access: access, Pins: pins, Projects: projects, Server: server, Reconciler: reconciler}, nil
}

// ValidateCredentialSeparation prevents a public bearer from obtaining the
// fixed service authority used for owner assertions and privileged reads.
// Check before opening any database or making a dependency request.
func (config RuntimeConfig) ValidateCredentialSeparation() error {
	if config.AuthMode != "access" && config.AuthMode != "dev" {
		return errors.New("explicit Generation authentication mode required")
	}
	if config.AuthMode == "access" {
		if err := authn.ConfiguredCookieNames(config.CookieName, config.GuestCookieName).Validate(); err != nil {
			return err
		}
		if config.Credentials != nil || config.DevFixtures || config.AccessURL == "" || config.AccessToken == "" {
			return errors.New("complete Access mode configuration required")
		}
		if _, err := authn.NewBrowserGuard(config.BrowserOrigins, config.LoopbackHTTP); err != nil {
			return err
		}
		return nil
	}
	if !config.DevFixtures || config.Credentials == nil {
		return errors.New("Generation caller credentials are required")
	}
	for _, token := range []string{config.StorageToken, config.AccessToken, config.ProjectsToken, config.OwnerCallbackToken} {
		if token == "" {
			continue
		}
		if _, public := config.Credentials.ResolveToken(token); public {
			return errors.New("public caller credentials must be distinct from service credentials")
		}
	}
	return nil
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
