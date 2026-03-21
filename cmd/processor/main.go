package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Tanilytics/processing/internal/config"
	"github.com/Tanilytics/processing/internal/consumer"
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

	// 5. Initialize event consumer
	eventConsumer, err := consumer.NewEventConsumer(
		cfg.RedpandaBrokers,
		cfg.ConsumerGroup,
		cfg.RedpandaSASLUser,
		cfg.RedpandaSASLPassword,
		cfg.RedpandaSASLMechanism,
		logger,
	)
	if err != nil {
		logger.Error("failed to initialize consumer", "error", err)
		return
	}

	var consumerWG sync.WaitGroup

	// 6. Start event consumer in a goroutine
	consumerWG.Go(func() {
		if err := eventConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("consumer stopped with error", "error", err)
			cancel()
		}
	})

	// 7. Start HTTP server in a goroutine
	go func() {
		logger.Info("http server listening", "addr", cfg.Port)
		if err := app.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "error", err)
			cancel()
		}
	}()

	// 8. Mark service as ready
	server.SetReady()
	logger.Info("service is ready")

	// 9. Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)
	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", "signal", sig)
	case <-ctx.Done():
		logger.Info("context cancelled, shutting down")
	}

	// 10. Graceful shutdown
	cancel()
	gracefulShutdown(app, logger)
	eventConsumer.Close()
	consumerWG.Wait()

	logger.Info("shutdown complete")
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
}
