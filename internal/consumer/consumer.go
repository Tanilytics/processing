package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Tanilytics/processing/internal/models"
	"github.com/Tanilytics/processing/internal/pipeline"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Options struct {
	Topic                  string
	ResetOffset            string
	FetchMinBytes          int32
	FetchMaxBytes          int32
	FetchMaxWait           time.Duration
	FetchMaxPartitionBytes int32
	MaxConcurrentFetches   int
	BatchSize              int
	BatchTimeout           time.Duration
}

type EventConsumer struct {
	client       *kgo.Client
	pipeline     *pipeline.Pipeline
	logger       *zerolog.Logger
	batchSize    int
	batchTimeout time.Duration
}

func NewEventConsumer(brokers []string, groupID, saslUser, saslPassword, saslMechanism string, options Options, p *pipeline.Pipeline, logger *zerolog.Logger) (*EventConsumer, error) {
	if strings.TrimSpace(options.Topic) == "" {
		return nil, fmt.Errorf("topic must not be empty")
	}

	resetOffset, err := parseResetOffset(options.ResetOffset)
	if err != nil {
		return nil, err
	}
	if options.FetchMinBytes < 0 {
		return nil, fmt.Errorf("fetch min bytes must be >= 0")
	}
	if options.FetchMaxBytes <= 0 {
		return nil, fmt.Errorf("fetch max bytes must be > 0")
	}
	if options.FetchMaxWait < 0 {
		return nil, fmt.Errorf("fetch max wait must be >= 0")
	}
	if options.FetchMaxPartitionBytes < 0 {
		return nil, fmt.Errorf("fetch max partition bytes must be >= 0")
	}
	if options.MaxConcurrentFetches < 0 {
		return nil, fmt.Errorf("max concurrent fetches must be >= 0")
	}
	if options.BatchSize <= 0 {
		return nil, fmt.Errorf("batch size must be > 0")
	}
	if options.BatchTimeout <= 0 {
		return nil, fmt.Errorf("batch timeout must be > 0")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(options.Topic),
		kgo.DisableAutoCommit(), // manual commit after processing
		kgo.FetchMinBytes(options.FetchMinBytes),
		kgo.FetchMaxBytes(options.FetchMaxBytes),
		kgo.FetchMaxWait(options.FetchMaxWait),
		kgo.FetchMaxPartitionBytes(options.FetchMaxPartitionBytes),
		kgo.MaxConcurrentFetches(options.MaxConcurrentFetches),
		kgo.ConsumeResetOffset(resetOffset),
	}

	if saslUser != "" {
		auth := scram.Auth{
			User: saslUser,
			Pass: saslPassword,
		}
		switch saslMechanism {
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
		default:
			// "SCRAM-SHA-256" and any unrecognized value fall back to SHA-256,
			// which is the Redpanda default.
			opts = append(opts, kgo.SASL(auth.AsSha256Mechanism()))
		}
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &EventConsumer{
		client:       client,
		pipeline:     p,
		logger:       logger,
		batchSize:    options.BatchSize,
		batchTimeout: options.BatchTimeout,
	}, nil
}

func parseResetOffset(value string) (kgo.Offset, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "end", "latest":
		return kgo.NewOffset().AtEnd(), nil
	case "start", "earliest":
		return kgo.NewOffset().AtStart(), nil
	default:
		return kgo.Offset{}, fmt.Errorf("invalid consumer reset offset %q: must be start or end", value)
	}
}

func (c *EventConsumer) Run(ctx context.Context) error {
	var (
		pending        []*models.InternalEvent
		batchStartedAt time.Time
	)

	for {
		if ctx.Err() != nil {
			if len(pending) == 0 {
				return ctx.Err()
			}

			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := c.flushPending(shutdownCtx, pending); err != nil {
				shutdownCancel()
				return fmt.Errorf("flush pending events on shutdown: %w", err)
			}

			shutdownCancel()
			return ctx.Err()
		}

		if shouldFlushBatch(len(pending), batchStartedAt, c.batchSize, c.batchTimeout, time.Now()) {
			if err := c.flushPending(ctx, pending); err != nil {
				c.logger.Error().Err(err).Int("batch_size", len(pending)).Msg("pipeline processing failed")
				continue
			}
			pending = nil
			batchStartedAt = time.Time{}
			continue
		}

		pollCtx := ctx
		cancel := func() {}
		if len(pending) > 0 {
			remaining := time.Until(batchStartedAt.Add(c.batchTimeout))
			if remaining <= 0 {
				continue
			}

			pollCtx, cancel = context.WithTimeout(ctx, remaining)
		}

		fetches := c.client.PollFetches(pollCtx)
		pollErr := pollCtx.Err()
		cancel()
		if ctx.Err() != nil {
			continue
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				c.logger.Error().
					Str("topic", e.Topic).
					Int32("partition", e.Partition).
					Err(e.Err).
					Msg("fetch error")
			}
		}

		batchWasEmpty := len(pending) == 0
		recordsFetched := 0
		fetches.EachRecord(func(record *kgo.Record) {
			recordsFetched++

			var event models.InternalEvent
			if err := json.Unmarshal(record.Value, &event); err != nil {
				c.logger.Error().
					Err(err).
					Int32("partition", record.Partition).
					Int64("offset", record.Offset).
					Msg("unmarshal event")
				return // skip malformed records (will be DLQ'd when I feel like doing it)
			}
			pending = append(pending, &event)
		})

		if batchWasEmpty && len(pending) > 0 {
			batchStartedAt = time.Now()
		}

		if len(pending) == 0 {
			if recordsFetched > 0 {
				if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
					c.logger.Error().Err(err).Msg("offset commit failed")
				}
			}

			continue
		}

		if shouldFlushBatch(len(pending), batchStartedAt, c.batchSize, c.batchTimeout, time.Now()) || pollErr == context.DeadlineExceeded {
			if err := c.flushPending(ctx, pending); err != nil {
				c.logger.Error().
					Err(err).
					Int("batch_size", len(pending)).
					Msg("pipeline processing failed")
				continue
			}

			pending = nil
			batchStartedAt = time.Time{}
		}
	}
}

func (c *EventConsumer) flushPending(ctx context.Context, events []*models.InternalEvent) error {
	if len(events) == 0 {
		return nil
	}

	if err := c.pipeline.Process(ctx, events); err != nil {
		return err
	}

	if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
		return fmt.Errorf("offset commit failed: %w", err)
	}

	return nil
}

func shouldFlushBatch(batchLen int, batchStartedAt time.Time, batchSize int, batchTimeout time.Duration, now time.Time) bool {
	if batchLen == 0 {
		return false
	}
	if batchLen >= batchSize {
		return true
	}
	if batchStartedAt.IsZero() {
		return false
	}

	return now.Sub(batchStartedAt) >= batchTimeout
}

func (c *EventConsumer) Close() {
	c.client.Close()
}
