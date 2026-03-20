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

	"github.com/Tanilytics/processing/internal/config"
	"github.com/Tanilytics/processing/internal/observability"
	"github.com/Tanilytics/processing/internal/server"
)

func main() {
	// 1. Load configuration
	cfg := config.LoadProcessorConfig()

	// 2. Init structured logger
	logger := observability.NewLogger("processor-service", cfg.LogLevel)
	logger.Info("starting processor service",
		"port", cfg.Port,
		"brokers", cfg.RedpandaBrokers,
		"redis_url", cfg.RedisURL,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Init OTel Tracer
	shutdownTracer, err := observability.InitTracer(ctx, "processor-service", cfg.OTelEndpoint)
	if err != nil {
		logger.Error("failed to initialize tracer", "error", err)
	} else {
		defer shutdownTracer()
	}

	// 4. Create HTTP server
	app := server.NewServer(cfg.Port)

	// 5. Mark service as ready
	server.SetReady()
	logger.Info("service is ready")

	// 6. Start HTTP server in a goroutine
	go func() {
		logger.Info("http server listening", "addr", cfg.Port)
		if err := app.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "error", err)
			cancel()
		}
	}()

	// 7. Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", "signal", sig)
	case <-ctx.Done():
		logger.Info("context cancelled, shutting down")
	}

	// 8. Graceful shutdown
	gracefulShutdown(app, logger)
}

func gracefulShutdown(
	app interface{ Shutdown(context.Context) error },
	logger *slog.Logger,
) {
	server.SetNotReady()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := app.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown error", "error", err)
	}

	logger.Info("shutdown complete")
}
