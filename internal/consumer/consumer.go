package consumer

import (
	"context"
	"encoding/json"
	"errors"
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
	BlockRebalanceOnPoll   bool
	MaxConcurrentFetches   int
	BatchSize              int
	BatchTimeout           time.Duration
}

type ConnectionOptions struct {
	GroupID string
	SASL    SASLOptions
}

type SASLOptions struct {
	User      string
	Password  string
	Mechanism string
}

type EventConsumer struct {
	client               *kgo.Client
	pipeline             *pipeline.Pipeline
	logger               *zerolog.Logger
	batchSize            int
	batchTimeout         time.Duration
	blockRebalanceOnPoll bool
}

type consumerBatch struct {
	pending   []*models.InternalEvent
	startedAt time.Time
}

func NewEventConsumer(brokers []string, connection ConnectionOptions, options Options, p *pipeline.Pipeline, logger *zerolog.Logger) (*EventConsumer, error) {
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
		kgo.ConsumerGroup(connection.GroupID),
		kgo.ConsumeTopics(options.Topic),
		kgo.DisableAutoCommit(), // manual commit after processing
		kgo.FetchMinBytes(options.FetchMinBytes),
		kgo.FetchMaxBytes(options.FetchMaxBytes),
		kgo.FetchMaxWait(options.FetchMaxWait),
		kgo.FetchMaxPartitionBytes(options.FetchMaxPartitionBytes),
		kgo.MaxConcurrentFetches(options.MaxConcurrentFetches),
		kgo.ConsumeResetOffset(resetOffset),
	}
	if options.BlockRebalanceOnPoll {
		opts = append(opts, kgo.BlockRebalanceOnPoll())
	}

	if connection.SASL.User != "" {
		auth := scram.Auth{
			User: connection.SASL.User,
			Pass: connection.SASL.Password,
		}
		switch connection.SASL.Mechanism {
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
		client:               client,
		pipeline:             p,
		logger:               logger,
		batchSize:            options.BatchSize,
		batchTimeout:         options.BatchTimeout,
		blockRebalanceOnPoll: options.BlockRebalanceOnPoll,
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
	if c.blockRebalanceOnPoll {
		return c.runWithBlockedRebalances(ctx)
	}

	batch := consumerBatch{}
	for {
		if err := c.runOnce(ctx, &batch); err != nil {
			return err
		}
	}
}

func (c *EventConsumer) runWithBlockedRebalances(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.runBlockedOnce(ctx); err != nil {
			return err
		}
	}
}

func (c *EventConsumer) runBlockedOnce(ctx context.Context) error {
	pollCtx, cancel := context.WithTimeout(ctx, c.batchTimeout)
	fetches := c.client.PollRecords(pollCtx, c.batchSize)
	cancel()
	defer c.client.AllowRebalance()

	if err := ctx.Err(); err != nil {
		return err
	}

	if isIgnorablePollError(fetches.Err()) && fetches.NumRecords() == 0 {
		return nil
	}

	batch := consumerBatch{}
	recordsFetched := c.collectPending(fetches, &batch)
	if len(batch.pending) == 0 {
		c.commitFetchedOffsets(ctx, recordsFetched)
		return nil
	}

	if err := c.flushPending(ctx, batch.pending); err != nil {
		c.logger.Error().Err(err).Int("batch_size", len(batch.pending)).Msg("pipeline processing failed")
	}

	return nil
}

func (c *EventConsumer) runOnce(ctx context.Context, batch *consumerBatch) error {
	if err := c.handleShutdown(ctx, batch); err != nil {
		return err
	}

	if c.flushReadyBatch(ctx, batch, nil, time.Now()) {
		return nil
	}

	fetches, pollErr, ok := c.pollFetches(ctx, batch)
	if !ok {
		return nil
	}

	recordsFetched := c.collectPending(fetches, batch)
	if len(batch.pending) == 0 {
		c.commitFetchedOffsets(ctx, recordsFetched)
		return nil
	}

	c.flushReadyBatch(ctx, batch, pollErr, time.Now())
	return nil
}

func (c *EventConsumer) handleShutdown(ctx context.Context, batch *consumerBatch) error {
	if ctx.Err() == nil {
		return nil
	}
	if len(batch.pending) == 0 {
		return ctx.Err()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()

	if err := c.flushPending(shutdownCtx, batch.pending); err != nil {
		return fmt.Errorf("flush pending events on shutdown: %w", err)
	}

	return ctx.Err()
}

func (c *EventConsumer) flushReadyBatch(ctx context.Context, batch *consumerBatch, pollErr error, now time.Time) bool {
	if !batch.shouldFlush(c.batchSize, c.batchTimeout, now) && pollErr != context.DeadlineExceeded {
		return false
	}

	if err := c.flushPending(ctx, batch.pending); err != nil {
		c.logger.Error().Err(err).Int("batch_size", len(batch.pending)).Msg("pipeline processing failed")
		return true
	}

	batch.reset()
	return true
}

func (c *EventConsumer) pollFetches(ctx context.Context, batch *consumerBatch) (kgo.Fetches, error, bool) {
	pollCtx := ctx
	var cancel context.CancelFunc
	if len(batch.pending) > 0 {
		remaining := time.Until(batch.startedAt.Add(c.batchTimeout))
		if remaining <= 0 {
			return kgo.Fetches{}, nil, false
		}

		pollCtx, cancel = context.WithTimeout(ctx, remaining)
	}

	fetches := c.client.PollFetches(pollCtx)
	pollErr := pollCtx.Err()
	if cancel != nil {
		cancel()
	}

	if ctx.Err() != nil {
		return fetches, pollErr, false
	}

	return fetches, pollErr, true
}

func (c *EventConsumer) collectPending(fetches kgo.Fetches, batch *consumerBatch) int {
	c.logFetchErrors(fetches)

	batchWasEmpty := len(batch.pending) == 0
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

		batch.pending = append(batch.pending, &event)
	})

	if batchWasEmpty && len(batch.pending) > 0 {
		batch.startedAt = time.Now()
	}

	return recordsFetched
}

func (c *EventConsumer) logFetchErrors(fetches kgo.Fetches) {
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, e := range errs {
			c.logger.Error().
				Str("topic", e.Topic).
				Int32("partition", e.Partition).
				Err(e.Err).
				Msg("fetch error")
		}
	}
}

func (c *EventConsumer) commitFetchedOffsets(ctx context.Context, recordsFetched int) {
	if recordsFetched == 0 {
		return
	}

	if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
		c.logger.Error().Err(err).Msg("offset commit failed")
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

func (b *consumerBatch) reset() {
	b.pending = nil
	b.startedAt = time.Time{}
}

func (b *consumerBatch) shouldFlush(batchSize int, batchTimeout time.Duration, now time.Time) bool {
	if len(b.pending) == 0 {
		return false
	}
	if len(b.pending) >= batchSize {
		return true
	}
	if b.startedAt.IsZero() {
		return false
	}

	return now.Sub(b.startedAt) >= batchTimeout
}

func (c *EventConsumer) Close() {
	c.client.Close()
}

func isIgnorablePollError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
