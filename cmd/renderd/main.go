package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Feruum/Leafrun/internal/renderer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("renderd stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := renderer.LoadConfig()
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if err := renderer.CheckRuntime(cfg); err != nil {
		return err
	}
	cache, err := renderer.NewCache(cfg)
	if err != nil {
		return err
	}
	if err := cache.Recover(); err != nil {
		return err
	}
	root, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{
		Addr: cfg.Addr, Handler: renderer.NewServer(cfg, cache, logger),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: cfg.RequestTimeout,
		WriteTimeout: cfg.RequestTimeout + time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 16 << 10,
	}
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	logger.Info("renderd listening", "address", cfg.Addr, "max_concurrent", cfg.MaxConcurrent)
	for {
		select {
		case <-root.Done():
			ctx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout+20*time.Second)
			defer cancel()
			err := server.Shutdown(ctx)
			if err != nil {
				_ = server.Close()
			}
			return err
		case err := <-errs:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case now := <-ticker.C:
			if err := cache.EvictIdle(now); err != nil {
				logger.Error("cache eviction failed", "code", "eviction_error")
			}
		}
	}
}
