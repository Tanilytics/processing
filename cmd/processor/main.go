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
	"github.com/Tanilytics/processing/internal/producer"
	"github.com/Tanilytics/processing/internal/server"
	"github.com/Tanilytics/processing/internal/storage"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

func newRedisClient(ctx context.Context, redisURL string, logger *zerolog.Logger) *redis.Client {
	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		logger.Error().Err(err).Msg("failed to parse redis URL")
		os.Exit(1)
	}

	redisClient := redis.NewClient(redisOpts)
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error().Err(err).Msg("failed to connect to redis")
		os.Exit(1)
	}

	logger.Info().Msg("connected to redis")
	return redisClient
}

func newHDFSWriter(cfg config.ProcessorConfig, logger *zerolog.Logger) (*storage.HDFSWriter, bool) {
	w, err := storage.NewHDFSWriter(storage.HDFSOptions{
		NameNodeAddr: cfg.HDFSNameNodeAddr,
		User:         cfg.HDFSUser,
		BasePath:     cfg.HDFSBasePath,
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize hdfs writer")
		return nil, false
	}

	return w, true
}

func newDLQProducer(cfg config.ProcessorConfig, logger *zerolog.Logger) (*producer.RedpandaProducer, error) {
	return producer.NewRedpandaProducer(
		cfg.RedpandaBrokers,
		producer.ConnectionOptions{
			SASL: producer.SASLOptions{
				User:      cfg.RedpandaSASLUser,
				Password:  cfg.RedpandaSASLPassword,
				Mechanism: cfg.RedpandaSASLMechanism,
			},
		},
		producer.Options{
			Topic:              cfg.DLQTopic,
			BatchMaxBytes:      cfg.DLQProducerBatchMaxBytes,
			Linger:             cfg.DLQProducerLinger,
			MaxBufferedRecords: cfg.DLQProducerMaxBufferedRecords,
			RecordRetries:      cfg.DLQProducerRecordRetries,
			RetryTimeout:       cfg.DLQProducerRetryTimeout,
		},
		logger,
	)
}

func newEventConsumer(
	cfg config.ProcessorConfig,
	dlqProducer *producer.RedpandaProducer,
	processorPipeline *pipeline.Pipeline,
	logger *zerolog.Logger,
) (*consumer.EventConsumer, error) {
	return consumer.NewEventConsumer(
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
			BatchSize:              cfg.ConsumerBatchSize,
			BatchTimeout:           cfg.ConsumerBatchTimeout,
		},
		dlqProducer,
		processorPipeline,
		logger,
	)
}

func main() {
	// 1. Load configuration
	cfg := config.LoadProcessorConfig()

	// 2. Init structured logger
	logger := observability.NewLogger("processor-service", cfg.LogLevel)
	logger.Info().
		Str("port", cfg.Port).
		Strs("brokers", cfg.RedpandaBrokers).
		Str("hdfs_namenode", cfg.HDFSNameNodeAddr).
		Str("redis_url", cfg.RedisURL).
		Msg("starting processor service")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Create HTTP server
	app := server.NewServer(cfg.Port)

	// 4. Initialize event consumer
	geoIP, err := processors.NewGeoIPResolver(cfg.GeoIPDBPath)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize geoip resolver")
		return
	}
	defer func() {
		//nolint:errcheck
		geoIP.Close()
	}()
	anonymizer := processors.NewAnonymizer(cfg.AnonymizationSalt, geoIP)
	uaParser := processors.NewUserAgentParser()
	redisClient := newRedisClient(ctx, cfg.RedisURL, logger)
	redisStore := storage.NewRedisStore(redisClient)

	defer func() {
		//nolint:errcheck
		redisClient.Close()
	}()

	sessionMgr := processors.NewSessionManager(redisClient)
	hdfsWriter, ok := newHDFSWriter(cfg, logger)
	if !ok {
		return
	}

	processorPipeline := pipeline.NewPipeline(anonymizer, uaParser, sessionMgr, hdfsWriter, redisStore, logger)
	dlqProducer, err := newDLQProducer(cfg, logger)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize dlq producer")
		//nolint:errcheck
		hdfsWriter.Close()
		return
	}

	eventConsumer, err := newEventConsumer(cfg, dlqProducer, processorPipeline, logger)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize consumer")
		//nolint:errcheck
		dlqProducer.Close()
		//nolint:errcheck
		hdfsWriter.Close()
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
	//nolint:errcheck
	eventConsumer.Close()
	//nolint:errcheck
	dlqProducer.Close()
	//nolint:errcheck
	hdfsWriter.Close()

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
