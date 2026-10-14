package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Feruum/Leafrun/internal/orchestration"
	"github.com/Feruum/Leafrun/internal/service"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "leafrun-generation:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string) error {
	if len(args) != 1 {
		return errors.New("expected one role: migrate, api, workflow, dispatcher, reconcile, controller, janitor or event-relay")
	}
	switch args[0] {
	case "migrate":
		return runMigrate(ctx, getenv)
	case "api":
		return runAPI(ctx, getenv)
	case "workflow":
		return runWorkflow(ctx, getenv)
	case "dispatcher":
		return runDispatcher(ctx, getenv)
	case "reconcile":
		return runReconcile(ctx, getenv)
	case "event-relay":
		return runEventRelay(ctx, getenv)
	case "controller":
		return runController(ctx, getenv)
	case "janitor":
		return runJanitor(ctx, getenv)
	default:
		return errors.New("unknown Generation process role")
	}
}

func runMigrate(ctx context.Context, getenv func(string) string) error {
	databaseURL, err := readDatabaseURL(getenv)
	if err != nil {
		return err
	}
	pool, state, err := service.OpenStore(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	return state.Migrate(ctx)
}

func runAPI(ctx context.Context, getenv func(string) string) error {
	config, err := readAPIEnvironment(getenv)
	if err != nil {
		return err
	}
	runtime, err := service.OpenRuntime(ctx, config.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close()
	return serveAPI(ctx, config.HTTP, runtime.Server)
}

func serveAPI(ctx context.Context, address string, handler http.Handler) error {
	server := &http.Server{
		Addr: address, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()
	select {
	case err := <-serveErr:
		return fmt.Errorf("Generation API server stopped: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return errors.New("Generation API did not shut down cleanly")
		}
		return nil
	}
}

func runWorkflow(ctx context.Context, getenv func(string) string) error {
	address, namespace, err := readTemporalEnvironment(getenv)
	if err != nil {
		return err
	}
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return errors.New("Temporal is unavailable")
	}
	defer temporalClient.Close()
	instance, err := orchestration.NewWorkflowWorker(temporalClient, worker.Options{})
	if err != nil {
		return err
	}
	if err = instance.Start(); err != nil {
		return errors.New("Generation workflow worker could not start")
	}
	<-ctx.Done()
	instance.Stop()
	return nil
}

func runDispatcher(ctx context.Context, getenv func(string) string) error {
	databaseURL, err := readDatabaseURL(getenv)
	if err != nil {
		return err
	}
	address, namespace, err := readTemporalEnvironment(getenv)
	if err != nil {
		return err
	}
	pool, state, err := service.OpenStore(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return errors.New("Temporal is unavailable")
	}
	defer temporalClient.Close()
	workerID, err := newWorkerID()
	if err != nil {
		return err
	}
	dispatcher, err := orchestration.NewDispatcher(state, temporalClient, workerID)
	if err != nil {
		return err
	}
	err = dispatcher.Run(ctx, time.Second)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func runReconcile(ctx context.Context, getenv func(string) string) error {
	config, err := readAPIEnvironment(getenv)
	if err != nil {
		return err
	}
	runtime, err := service.OpenRuntime(ctx, config.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close()
	err = runtime.Reconciler.Run(ctx)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func newWorkerID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", errors.New("could not create Temporal dispatcher identity")
	}
	return "generation-dispatcher-" + hex.EncodeToString(raw[:]), nil
}
