package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	invalidEnvVarLogFormat = "invalid value for environment variable %s: %v"
)

type ProcessorConfig struct {
	Port                 string
	RedpandaBrokers      []string
	RedpandaSASLUser     string
	RedpandaSASLPassword string
	// RedpandaSASLMechanism is the SASL mechanism to use.
	// Supported values: "SCRAM-SHA-256" (default), "SCRAM-SHA-512".
	// Leave empty to disable SASL.
	RedpandaSASLMechanism          string
	ConsumerGroup                  string
	ConsumerTopic                  string
	ConsumerResetOffset            string
	ConsumerFetchMinBytes          int32
	ConsumerFetchMaxBytes          int32
	ConsumerFetchMaxWait           time.Duration
	ConsumerFetchMaxPartitionBytes int32
	ConsumerBlockRebalanceOnPoll   bool
	ConsumerMaxConcurrentFetches   int
	ClickhouseAddrs                []string
	ClickhouseDatabase             string
	ClickhouseUsername             string
	ClickhousePassword             string
	ClickhouseDialTimeout          time.Duration
	ClickhouseBatchSize            int
	ClickhouseBatchTimeout         time.Duration
	ClickhouseMaxOpenConns         int
	ClickhouseMaxIdleConns         int
	ClickhouseConnOpenStrategy     string
	RedisURL                       string
	AnonymizationSalt              string
	LogLevel                       string
}

func LoadProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		Port:                  getEnv("PROCESSING_PORT"),
		RedpandaBrokers:       strings.Split(getEnv("REDPANDA_BROKERS"), ","),
		RedpandaSASLUser:      getEnv("REDPANDA_SASL_USER"),
		RedpandaSASLPassword:  getEnv("REDPANDA_SASL_PASSWORD"),
		RedpandaSASLMechanism: getEnv("REDPANDA_SASL_MECHANISM"),
		ConsumerGroup:         getEnv("CONSUMER_GROUP"),
		ConsumerTopic:         getEnv("CONSUMER_TOPIC"),
		ConsumerResetOffset:   getEnv("CONSUMER_RESET_OFFSET"),
		ConsumerFetchMinBytes: getEnvInt32("CONSUMER_FETCH_MIN_BYTES"),
		ConsumerFetchMaxBytes: getEnvInt32("CONSUMER_FETCH_MAX_BYTES"),
		ConsumerFetchMaxWait:  getEnvDuration("CONSUMER_FETCH_MAX_WAIT"),
		ConsumerFetchMaxPartitionBytes: getEnvInt32(
			"CONSUMER_FETCH_MAX_PARTITION_BYTES",
		),
		ConsumerBlockRebalanceOnPoll: getEnvBool(
			"CONSUMER_BLOCK_REBALANCE_ON_POLL",
		),
		ConsumerMaxConcurrentFetches: getEnvInt("CONSUMER_MAX_CONCURRENT_FETCHES"),
		ClickhouseAddrs:              getEnvCSV("CLICKHOUSE_ADDRS"),
		ClickhouseDatabase:           getEnv("CLICKHOUSE_DATABASE"),
		ClickhouseUsername:           getEnv("CLICKHOUSE_USERNAME"),
		ClickhousePassword:           getEnv("CLICKHOUSE_PASSWORD"),
		ClickhouseDialTimeout:        getEnvDuration("CLICKHOUSE_DIAL_TIMEOUT"),
		ClickhouseBatchSize:          getEnvInt("CH_BATCH_SIZE"),
		ClickhouseBatchTimeout:       getEnvDuration("CH_BATCH_TIMEOUT"),
		ClickhouseMaxOpenConns:       getEnvInt("CLICKHOUSE_MAX_OPEN_CONNS"),
		ClickhouseMaxIdleConns:       getEnvInt("CLICKHOUSE_MAX_IDLE_CONNS"),
		ClickhouseConnOpenStrategy:   getEnv("CLICKHOUSE_CONN_OPEN_STRATEGY"),
		RedisURL:                     getEnv("REDIS_URL"),
		LogLevel:                     getEnv("LOG_LEVEL"),
		AnonymizationSalt:            getEnv("ANONYMIZATION_SALT"),
	}
}

func getEnv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		log.Printf("missing required environment variable: %s", key)
		os.Exit(1)
	}

	return v
}

func getEnvInt(key string) int {
	v := getEnv(key)

	i, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		log.Printf(invalidEnvVarLogFormat, key, err)
		os.Exit(1)
	}

	return i
}

func getEnvInt32(key string) int32 {
	v := getEnv(key)

	i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 32)
	if err != nil {
		log.Printf(invalidEnvVarLogFormat, key, err)
		os.Exit(1)
	}

	return int32(i)
}

func getEnvDuration(key string) time.Duration {
	v := getEnv(key)

	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		log.Printf(invalidEnvVarLogFormat, key, err)
		os.Exit(1)
	}

	return d
}

func getEnvBool(key string) bool {
	v := getEnv(key)

	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		log.Printf(invalidEnvVarLogFormat, key, err)
		os.Exit(1)
	}

	return b
}

func getEnvCSV(key string) []string {
	v := getEnv(key)
	parts := strings.Split(v, ",")
	values := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			values = append(values, trimmed)
		}
	}

	if len(values) == 0 {
		log.Printf("invalid value for environment variable %s: must contain at least one address", key)
		os.Exit(1)
	}

	return values
}
