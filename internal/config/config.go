package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type ProcessorConfig struct {
	Port                 string
	RedpandaBrokers      []string
	RedpandaSASLUser     string
	RedpandaSASLPassword string
	// RedpandaSASLMechanism is the SASL mechanism to use.
	// Supported values: "SCRAM-SHA-256" (default), "SCRAM-SHA-512".
	// Leave empty to disable SASL.
	RedpandaSASLMechanism  string
	ConsumerGroup          string
	ClickhouseAddr         string
	ClickhouseBatchSize    int
	ClickhouseBatchTimeout time.Duration
	RedisURL               string
	AnonymizationSalt      string
	LogLevel               string
	OTelEndpoint           string
}

func LoadProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		Port:                   getEnv("PROCESSING_PORT"),
		RedpandaBrokers:        strings.Split(getEnv("REDPANDA_BROKERS"), ","),
		RedpandaSASLUser:       getEnv("REDPANDA_SASL_USER"),
		RedpandaSASLPassword:   getEnv("REDPANDA_SASL_PASSWORD"),
		RedpandaSASLMechanism:  getEnv("REDPANDA_SASL_MECHANISM"),
		ConsumerGroup:          getEnv("CONSUMER_GROUP"),
		ClickhouseAddr:         getEnv("CLICKHOUSE_ADDR"),
		ClickhouseBatchSize:    getPositiveIntEnv("CH_BATCH_SIZE"),
		ClickhouseBatchTimeout: getDurationEnv("CH_BATCH_TIMEOUT"),
		RedisURL:               getEnv("REDIS_URL"),
		LogLevel:               getEnv("LOG_LEVEL"),
		AnonymizationSalt:      getEnv("ANONYMIZATION_SALT"),
		OTelEndpoint:           getEnv("OTEL_EXPORTER_OTLP_ENDPOINT"),
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

func getPositiveIntEnv(key string) int {
	raw := getEnv(key)
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		log.Printf("invalid environment variable %s: %q", key, raw)
		os.Exit(1)
	}

	return v
}

func getDurationEnv(key string) time.Duration {
	raw := getEnv(key)
	v, err := time.ParseDuration(raw)
	if err != nil || v <= 0 {
		log.Printf("invalid environment variable %s: %q", key, raw)
		os.Exit(1)
	}

	return v
}
