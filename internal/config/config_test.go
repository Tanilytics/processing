package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLoadProcessorConfig(t *testing.T) {
	t.Setenv("PROCESSING_PORT", ":3000")
	t.Setenv("REDPANDA_BROKERS", "broker-1:9092,broker-2:9092")
	t.Setenv("REDPANDA_SASL_USER", "superuser")
	t.Setenv("REDPANDA_SASL_PASSWORD", "secretpassword")
	t.Setenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256")
	t.Setenv("CONSUMER_GROUP", "event-processor")
	t.Setenv("CONSUMER_TOPIC", "raw-events")
	t.Setenv("CONSUMER_RESET_OFFSET", "end")
	t.Setenv("CONSUMER_FETCH_MIN_BYTES", "65536")
	t.Setenv("CONSUMER_FETCH_MAX_BYTES", "52428800")
	t.Setenv("CONSUMER_FETCH_MAX_WAIT", "250ms")
	t.Setenv("CONSUMER_FETCH_MAX_PARTITION_BYTES", "4194304")
	t.Setenv("CONSUMER_MAX_CONCURRENT_FETCHES", "2")
	t.Setenv("CLICKHOUSE_ADDRS", "clickhouse-1:9000, clickhouse-2:9000")
	t.Setenv("CLICKHOUSE_DATABASE", "default")
	t.Setenv("CLICKHOUSE_USERNAME", "default")
	t.Setenv("CLICKHOUSE_PASSWORD", "change-me")
	t.Setenv("CLICKHOUSE_MAX_OPEN_CONNS", "10")
	t.Setenv("CLICKHOUSE_MAX_IDLE_CONNS", "5")
	t.Setenv("CLICKHOUSE_CONN_OPEN_STRATEGY", "round_robin")
	t.Setenv("CH_BATCH_SIZE", "10000")
	t.Setenv("CH_BATCH_TIMEOUT", "5s")
	t.Setenv("REDIS_URL", "redis://localhost:6379")
	t.Setenv("ANONYMIZATION_SALT", "change-me")
	t.Setenv("LOG_LEVEL", "info")

	cfg := LoadProcessorConfig()

	assert.Equal(t, ":3000", cfg.Port)
	assert.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, cfg.RedpandaBrokers)
	assert.Equal(t, "event-processor", cfg.ConsumerGroup)
	assert.Equal(t, "raw-events", cfg.ConsumerTopic)
	assert.Equal(t, "end", cfg.ConsumerResetOffset)
	assert.Equal(t, int32(65536), cfg.ConsumerFetchMinBytes)
	assert.Equal(t, int32(52428800), cfg.ConsumerFetchMaxBytes)
	assert.Equal(t, 250*time.Millisecond, cfg.ConsumerFetchMaxWait)
	assert.Equal(t, int32(4194304), cfg.ConsumerFetchMaxPartitionBytes)
	assert.Equal(t, 2, cfg.ConsumerMaxConcurrentFetches)
	assert.Equal(t, []string{"clickhouse-1:9000", "clickhouse-2:9000"}, cfg.ClickhouseAddrs)
	assert.Equal(t, 10, cfg.ClickhouseMaxOpenConns)
	assert.Equal(t, 5, cfg.ClickhouseMaxIdleConns)
	assert.Equal(t, "round_robin", cfg.ClickhouseConnOpenStrategy)
	assert.Equal(t, 10000, cfg.ClickhouseBatchSize)
	assert.Equal(t, 5*time.Second, cfg.ClickhouseBatchTimeout)
}
