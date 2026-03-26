package producer

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Options struct {
	Topic              string
	BatchMaxBytes      int32
	Linger             time.Duration
	MaxBufferedRecords int
	RecordRetries      int
	RetryTimeout       time.Duration
}

type ConnectionOptions struct {
	SASL SASLOptions
}

type SASLOptions struct {
	User      string
	Password  string
	Mechanism string
}

type RedpandaProducer struct {
	client *kgo.Client
	logger *zerolog.Logger
	topic  string
}

func NewRedpandaProducer(
	brokers []string,
	connection ConnectionOptions,
	options Options,
	logger *zerolog.Logger,
) (*RedpandaProducer, error) {
	topic := strings.TrimSpace(options.Topic)
	if topic == "" {
		return nil, fmt.Errorf("topic must not be empty")
	}
	if options.BatchMaxBytes <= 0 {
		return nil, fmt.Errorf("batch max bytes must be > 0")
	}
	if options.Linger < 0 {
		return nil, fmt.Errorf("linger must be >= 0")
	}
	if options.MaxBufferedRecords <= 0 {
		return nil, fmt.Errorf("max buffered records must be > 0")
	}
	if options.RecordRetries < 0 {
		return nil, fmt.Errorf("record retries must be >= 0")
	}
	if options.RetryTimeout <= 0 {
		return nil, fmt.Errorf("retry timeout must be > 0")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerBatchMaxBytes(options.BatchMaxBytes),
		kgo.ProducerLinger(options.Linger),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.MaxBufferedRecords(options.MaxBufferedRecords),
		kgo.RecordRetries(options.RecordRetries),
		kgo.RetryTimeout(options.RetryTimeout),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelWarn, nil)),
	}

	if connection.SASL.User != "" {
		auth := scram.Auth{
			User: connection.SASL.User,
			Pass: connection.SASL.Password,
		}
		switch strings.TrimSpace(connection.SASL.Mechanism) {
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
		default:
			opts = append(opts, kgo.SASL(auth.AsSha256Mechanism()))
		}
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &RedpandaProducer{client: client, logger: logger, topic: topic}, nil
}

func (p *RedpandaProducer) ProduceSync(
	ctx context.Context,
	key string,
	value []byte,
	headers []kgo.RecordHeader,
) error {
	record := &kgo.Record{
		Topic:   p.topic,
		Key:     []byte(key),
		Value:   append([]byte(nil), value...),
		Headers: cloneHeaders(headers),
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		if p.logger != nil {
			p.logger.Error().Err(err).Str("topic", p.topic).Msg("sync produce failed")
		}
		return fmt.Errorf("produce to topic %q: %w", p.topic, err)
	}

	return nil
}

func cloneHeaders(headers []kgo.RecordHeader) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}

	cloned := make([]kgo.RecordHeader, 0, len(headers))
	for _, header := range headers {
		cloned = append(cloned, kgo.RecordHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		})
	}

	return cloned
}

func (p *RedpandaProducer) Close() {
	p.client.Flush(context.Background()) // nolint: errcheck
	p.client.Close()
}
