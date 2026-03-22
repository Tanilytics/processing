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
}

type EventConsumer struct {
	client   *kgo.Client
	pipeline *pipeline.Pipeline
	logger   *zerolog.Logger
}

func NewEventConsumer(brokers []string, groupID, saslUser, saslPassword, saslMechanism string, options Options, p *pipeline.Pipeline, logger *zerolog.Logger) (*EventConsumer, error) {
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
	return &EventConsumer{client: client, pipeline: p, logger: logger}, nil
}

func parseResetOffset(value string) (kgo.Offset, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "end", "latest":
		return kgo.NewOffset().AtEnd(), nil
	case "start", "earliest":
		return kgo.NewOffset().AtStart(), nil
	default:
		return kgo.Offset{}, fmt.Errorf("invalid consumer reset offset %q: must be start or end", value)
	}
}

func (c *EventConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
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

		var events []*models.InternalEvent
		fetches.EachRecord(func(record *kgo.Record) {
			var event models.InternalEvent
			if err := json.Unmarshal(record.Value, &event); err != nil {
				c.logger.Error().
					Err(err).
					Int32("partition", record.Partition).
					Int64("offset", record.Offset).
					Msg("unmarshal event")
				return // skip malformed records (will be DLQ'd when I feel like doing it)
			}
			events = append(events, &event)
		})

		if len(events) > 0 {
			if err := c.pipeline.Process(ctx, events); err != nil {
				c.logger.Error().
					Err(err).
					Int("batch_size", len(events)).
					Msg("pipeline processing failed")
				// Don't commit offsets, will retry on next poll
				continue
			}
		}

		// Commit offsets after successful processing
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.logger.Error().Err(err).Msg("offset commit failed")
		}
	}
}

func (c *EventConsumer) Close() {
	c.client.Close()
}
