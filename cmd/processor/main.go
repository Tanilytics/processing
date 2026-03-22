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
		Strs("clickhouse_addrs", cfg.ClickhouseAddrs).
		Str("redis_url", cfg.RedisURL).
		Msg("starting processor service")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Create HTTP server
	app := server.NewServer(cfg.Port)

	// 4. Initialize event consumer
	anonymizer := processors.NewAnonymizer(cfg.AnonymizationSalt)
	uaParser := processors.NewUserAgentParser()
	chWriter, err := storage.NewClickHouseWriter(storage.Options{
		Addrs:            cfg.ClickhouseAddrs,
		Database:         cfg.ClickhouseDatabase,
		Username:         cfg.ClickhouseUsername,
		Password:         cfg.ClickhousePassword,
		MaxOpenConns:     cfg.ClickhouseMaxOpenConns,
		MaxIdleConns:     cfg.ClickhouseMaxIdleConns,
		ConnOpenStrategy: cfg.ClickhouseConnOpenStrategy,
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize clickhouse writer")
		return
	}

	processorPipeline := pipeline.NewPipeline(anonymizer, uaParser, chWriter, logger)

	eventConsumer, err := consumer.NewEventConsumer(
		cfg.RedpandaBrokers,
		consumer.ConnectionOptions{
			GroupID: cfg.ConsumerGroup,
			SASL: consumer.SASLOptions{
				User:      cfg.RedpandaSASLUser,
				Password:  cfg.RedpandaSASLPassword,
				Mechanism: cfg.RedpandaSASLMechanism,
			},
		},
		consumer.Options{
			Topic:                  cfg.ConsumerTopic,
			ResetOffset:            cfg.ConsumerResetOffset,
			FetchMinBytes:          cfg.ConsumerFetchMinBytes,
			FetchMaxBytes:          cfg.ConsumerFetchMaxBytes,
			FetchMaxWait:           cfg.ConsumerFetchMaxWait,
			FetchMaxPartitionBytes: cfg.ConsumerFetchMaxPartitionBytes,
			BlockRebalanceOnPoll:   cfg.ConsumerBlockRebalanceOnPoll,
			MaxConcurrentFetches:   cfg.ConsumerMaxConcurrentFetches,
			BatchSize:              cfg.ClickhouseBatchSize,
			BatchTimeout:           cfg.ClickhouseBatchTimeout,
		},
		processorPipeline,
		logger,
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize consumer")
		return
	}

	var consumerWG sync.WaitGroup

	// 5. Start event consumer in a goroutine
	consumerWG.Go(func() {
		if err := eventConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error().Err(err).Msg("consumer stopped with error")
			cancel()
		}
	})

	// 6. Start HTTP server in a goroutine
	go func() {
		logger.Info().Str("addr", cfg.Port).Msg("http server listening")
		if err := app.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("http server error")
			cancel()
		}
	}()

	// 7. Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)
	select {
	case sig := <-sigCh:
		logger.Info().Stringer("signal", sig).Msg("received signal, shutting down")
	case <-ctx.Done():
		logger.Info().Msg("context cancelled, shutting down")
	}

	// 8. Graceful shutdown
	cancel()
	gracefulShutdown(app, logger)
	consumerWG.Wait()
	eventConsumer.Close()
	if err := chWriter.Close(); err != nil {
		logger.Error().Err(err).Msg("clickhouse writer shutdown error")
	}

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
