package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Tanilytics/processing/internal/config"
	"github.com/Tanilytics/processing/internal/consumer"
	"github.com/Tanilytics/processing/internal/observability"
	"github.com/Tanilytics/processing/internal/pipeline"
	"github.com/Tanilytics/processing/internal/processors"
	"github.com/Tanilytics/processing/internal/server"
	"github.com/Tanilytics/processing/internal/storage"
	"github.com/rs/zerolog"
)

func main() {
	// 1. Load configuration
	cfg := config.LoadProcessorConfig()

	// 2. Init structured logger
	logger := observability.NewLogger("processor-service", cfg.LogLevel)
	logger.Info().
		Str("port", cfg.Port).
		Strs("brokers", cfg.RedpandaBrokers).
		Str("clickhouse_addr", cfg.ClickhouseAddr).
		Str("redis_url", cfg.RedisURL).
		Msg("starting processor service")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Init OTel Tracer
	shutdownTracer, err := observability.InitTracer(ctx, "processor-service", cfg.OTelEndpoint)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize tracer")
	} else {
		defer shutdownTracer()
	}

	// 4. Create HTTP server
	app := server.NewServer(cfg.Port)

	// 5. Initialize event consumer
	anonymizer := processors.NewAnonymizer(cfg.AnonymizationSalt)
	uaParser := processors.NewUserAgentParser()
	chWriter, err := storage.NewClickHouseWriter(cfg.ClickhouseAddr)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize clickhouse writer")
		return
	}
	defer chWriter.Close() // nolint: errcheck

	processorPipeline := pipeline.NewPipeline(anonymizer, uaParser, chWriter, logger)

	eventConsumer, err := consumer.NewEventConsumer(
		cfg.RedpandaBrokers,
		cfg.ConsumerGroup,
		cfg.RedpandaSASLUser,
		cfg.RedpandaSASLPassword,
		cfg.RedpandaSASLMechanism,
		processorPipeline,
		logger,
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize consumer")
		return
	}

	var consumerWG sync.WaitGroup

	// 6. Start event consumer in a goroutine
	consumerWG.Go(func() {
		if err := eventConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error().Err(err).Msg("consumer stopped with error")
			cancel()
		}
	})

	// 7. Start HTTP server in a goroutine
	go func() {
		logger.Info().Str("addr", cfg.Port).Msg("http server listening")
		if err := app.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("http server error")
			cancel()
		}
	}()

	// 8. Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)
	select {
	case sig := <-sigCh:
		logger.Info().Stringer("signal", sig).Msg("received signal, shutting down")
	case <-ctx.Done():
		logger.Info().Msg("context cancelled, shutting down")
	}

	// 9. Graceful shutdown
	cancel()
	gracefulShutdown(app, logger)
	consumerWG.Wait()
	eventConsumer.Close()

	logger.Info().Msg("shutdown complete")
}

func gracefulShutdown(
	app interface{ Shutdown(context.Context) error },
	logger *zerolog.Logger,
) {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := app.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("http server shutdown error")
	}
}
