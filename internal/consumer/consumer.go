package consumer

import (
	"context"
	"encoding/json"

	"github.com/Tanilytics/processing/internal/models"
	"github.com/Tanilytics/processing/internal/pipeline"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type EventConsumer struct {
	client   *kgo.Client
	pipeline *pipeline.Pipeline
	logger   *zerolog.Logger
}

func NewEventConsumer(brokers []string, groupID, saslUser, saslPassword, saslMechanism string, p *pipeline.Pipeline, logger *zerolog.Logger) (*EventConsumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics("raw-events"),
		kgo.DisableAutoCommit(), // manual commit after processing
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(50 * 1024 * 1024),               // 50MB max fetch
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // start from beginning on new group
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
