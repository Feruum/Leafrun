package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"path/filepath"
	"regexp"
	"time"

	"github.com/Feruum/Leafrun/internal/controller"
	"github.com/Feruum/Leafrun/internal/orchestration"
	"github.com/Feruum/Leafrun/internal/sandbox"
	"github.com/Feruum/Leafrun/internal/service"
	"github.com/Feruum/Leafrun/internal/store"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func openControllerRuntime(ctx context.Context, getenv func(string) string) (*service.Runtime, *storagev1.Client, *sandbox.Docker, controller.Config, error) {
	var cfg controller.Config
	env, err := readAPIEnvironment(getenv)
	if err != nil {
		return nil, nil, nil, cfg, err
	}
	cfg.StagingRoot, err = required(getenv, "GENERATION_STAGING_ROOT")
	if err != nil {
		return nil, nil, nil, cfg, err
	}
	cfg.RendererImage, err = required(getenv, "GENERATION_RENDERER_IMAGE")
	if err != nil {
		return nil, nil, nil, cfg, err
	}
	if !filepath.IsAbs(cfg.StagingRoot) || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(cfg.RendererImage) {
		return nil, nil, nil, cfg, errors.New("absolute staging root and immutable local renderer image are required")
	}
	cfg.OwnerID, err = newWorkerID()
	if err != nil {
		return nil, nil, nil, cfg, err
	}
	runtime, err := service.OpenRuntime(ctx, env.Runtime)
	if err != nil {
		return nil, nil, nil, cfg, err
	}
	cfg.Projects = runtime.Projects
	storage, err := storagev1.NewClient(env.Runtime.StorageURL, env.Runtime.StorageToken, &http.Client{Timeout: 2 * time.Minute})
	if err != nil {
		runtime.Close()
		return nil, nil, nil, cfg, errors.New("Storage configuration is invalid")
	}
	engine, err := sandbox.NewDocker()
	if err != nil {
		runtime.Close()
		return nil, nil, nil, cfg, errors.New("Docker client could not start")
	}
	ready, stop := context.WithTimeout(ctx, 10*time.Second)
	err = engine.Ready(ready)
	stop()
	if err != nil {
		engine.Close()
		runtime.Close()
		return nil, nil, nil, cfg, errors.New("Linux Docker resource limits are unavailable")
	}
	return runtime, storage, engine, cfg, nil
}

func runController(ctx context.Context, getenv func(string) string) error {
	runtime, storage, engine, cfg, err := openControllerRuntime(ctx, getenv)
	if err != nil {
		return err
	}
	defer runtime.Close()
	defer engine.Close()
	execute, err := controller.New(runtime.Store, storage, engine, runtime.Server, runtime.Access, cfg)
	if err != nil {
		return err
	}
	address, namespace, err := readTemporalEnvironment(getenv)
	if err != nil {
		return err
	}
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return errors.New("Temporal is unavailable")
	}
	defer temporalClient.Close()
	instance, err := orchestration.NewActivityWorker(temporalClient, worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 2}, execute.Execute)
	if err != nil {
		return err
	}
	if err = instance.Start(); err != nil {
		return errors.New("Generation activity worker could not start")
	}
	defer instance.Stop()
	// Publishing is recoverable from PostgreSQL even if a Temporal Activity
	// exhausted retries after an unknown HTTP outcome. Queued renders still
	// enter exclusively through their durable workflow start.
	recoveryDone := make(chan struct{})
	go func() { defer close(recoveryDone); recoverPublications(ctx, runtime.Store, execute) }()
	<-ctx.Done()
	<-recoveryDone
	return nil
}

func recoverPublications(ctx context.Context, state *store.Store, execute *controller.Controller) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		ids, err := state.PendingExecutionJobs(ctx, 100)
		if err == nil {
			for _, id := range ids {
				job, getErr := state.GetJob(ctx, id)
				if getErr != nil || job.Status != "publishing" {
					continue
				}
				call, cancel := context.WithTimeout(ctx, 2*time.Minute)
				_ = execute.Execute(call, id)
				cancel()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runJanitor(ctx context.Context, getenv func(string) string) error {
	runtime, storage, engine, cfg, err := openControllerRuntime(ctx, getenv)
	if err != nil {
		return err
	}
	defer runtime.Close()
	defer engine.Close()
	janitor, err := controller.NewJanitor(runtime.Store, storage, engine, cfg)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, 25*time.Second)
		err = janitor.RunOnce(call)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Print("Generation janitor will retry unresolved cleanup")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	return nil
}
