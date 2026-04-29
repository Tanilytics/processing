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
	DLQTopic                       string
	DLQProducerBatchMaxBytes       int32
	DLQProducerLinger              time.Duration
	DLQProducerMaxBufferedRecords  int
	DLQProducerRecordRetries       int
	DLQProducerRetryTimeout        time.Duration
	ConsumerGroup                  string
	ConsumerTopic                  string
	ConsumerResetOffset            string
	ConsumerFetchMinBytes          int32
	ConsumerFetchMaxBytes          int32
	ConsumerFetchMaxWait           time.Duration
	ConsumerFetchMaxPartitionBytes int32
	ConsumerBlockRebalanceOnPoll   bool
	ConsumerMaxConcurrentFetches   int
	ConsumerBatchSize              int
	ConsumerBatchTimeout           time.Duration
	HDFSNameNodeAddr               string
	HDFSUser                       string
	HDFSBasePath                   string
	RedisURL                       string
	LogLevel                       string
	AnonymizationSalt              string
	GeoIPDBPath                    string
}

func LoadProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		Port:                          getEnv("PROCESSING_PORT"),
		RedpandaBrokers:               strings.Split(getEnv("REDPANDA_BROKERS"), ","),
		RedpandaSASLUser:              getEnv("REDPANDA_SASL_USER"),
		RedpandaSASLPassword:          getEnv("REDPANDA_SASL_PASSWORD"),
		RedpandaSASLMechanism:         getEnv("REDPANDA_SASL_MECHANISM"),
		DLQTopic:                      getEnv("DLQ_TOPIC"),
		DLQProducerBatchMaxBytes:      getEnvInt32("DLQ_PRODUCER_BATCH_MAX_BYTES"),
		DLQProducerLinger:             getEnvDuration("DLQ_PRODUCER_LINGER"),
		DLQProducerMaxBufferedRecords: getEnvInt("DLQ_PRODUCER_MAX_BUFFERED_RECORDS"),
		DLQProducerRecordRetries:      getEnvInt("DLQ_PRODUCER_RECORD_RETRIES"),
		DLQProducerRetryTimeout:       getEnvDuration("DLQ_PRODUCER_RETRY_TIMEOUT"),
		ConsumerGroup:                 getEnv("CONSUMER_GROUP"),
		ConsumerTopic:                 getEnv("CONSUMER_TOPIC"),
		ConsumerResetOffset:           getEnv("CONSUMER_RESET_OFFSET"),
		ConsumerFetchMinBytes:         getEnvInt32("CONSUMER_FETCH_MIN_BYTES"),
		ConsumerFetchMaxBytes:         getEnvInt32("CONSUMER_FETCH_MAX_BYTES"),
		ConsumerFetchMaxWait:          getEnvDuration("CONSUMER_FETCH_MAX_WAIT"),
		ConsumerFetchMaxPartitionBytes: getEnvInt32(
			"CONSUMER_FETCH_MAX_PARTITION_BYTES",
		),
		ConsumerBlockRebalanceOnPoll: getEnvBool(
			"CONSUMER_BLOCK_REBALANCE_ON_POLL",
		),
		ConsumerMaxConcurrentFetches: getEnvInt("CONSUMER_MAX_CONCURRENT_FETCHES"),
		ConsumerBatchSize:            getEnvInt("CONSUMER_BATCH_SIZE"),
		ConsumerBatchTimeout:         getEnvDuration("CONSUMER_BATCH_TIMEOUT"),
		HDFSNameNodeAddr:             getEnv("HDFS_NAMENODE_ADDR"),
		HDFSUser:                     getEnv("HDFS_USER"),
		HDFSBasePath:                 getEnv("HDFS_BASE_PATH"),
		RedisURL:                     getEnv("REDIS_URL"),
		LogLevel:                     getEnv("LOG_LEVEL"),
		AnonymizationSalt:            getEnv("ANONYMIZATION_SALT"),
		GeoIPDBPath:                  getEnv("GEOIP_DB_PATH"),
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
	return parseInt(key, v)
}

func parseInt(key, value string) int {
	i, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		log.Printf(invalidEnvVarLogFormat, key, err)
		os.Exit(1)
	}

	return i
}

func getEnvInt32(key string) int32 {
	v := getEnv(key)
	return parseInt32(key, v)
}

func parseInt32(key, value string) int32 {
	i, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil {
		log.Printf(invalidEnvVarLogFormat, key, err)
		os.Exit(1)
	}

	return int32(i)
}

func getEnvDuration(key string) time.Duration {
	v := getEnv(key)
	return parseDuration(key, v)
}

func parseDuration(key, value string) time.Duration {
	d, err := time.ParseDuration(strings.TrimSpace(value))
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
