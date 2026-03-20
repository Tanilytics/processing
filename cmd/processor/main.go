package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Tanilytics/processing/internal/config"
	"github.com/Tanilytics/processing/internal/observability"
)

func main() {
	// 1. Load configuration
	cfg := config.LoadProcessorConfig()

	// 2. Init structured logger
	logger := observability.NewLogger("processor-service", cfg.LogLevel)
	logger.Info("starting processor service",
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

	// 4. Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", "signal", sig)
	case <-ctx.Done():
		logger.Info("context cancelled, shutting down")
	}
}
