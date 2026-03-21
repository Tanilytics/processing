package config

import (
	"log"
	"os"
	"strings"
)

type ProcessorConfig struct {
	Port                 string
	RedpandaBrokers      []string
	RedpandaSASLUser     string
	RedpandaSASLPassword string
	// RedpandaSASLMechanism is the SASL mechanism to use.
	// Supported values: "SCRAM-SHA-256" (default), "SCRAM-SHA-512".
	// Leave empty to disable SASL.
	RedpandaSASLMechanism string
	RedisURL              string
	LogLevel              string
	OTelEndpoint          string
	ConsumerGroup         string
}

func LoadProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		Port:                  getEnv("PROCESSING_PORT"),
		RedpandaBrokers:       strings.Split(getEnv("REDPANDA_BROKERS"), ","),
		RedpandaSASLUser:      getEnv("REDPANDA_SASL_USER"),
		RedpandaSASLPassword:  getEnv("REDPANDA_SASL_PASSWORD"),
		RedpandaSASLMechanism: getEnv("REDPANDA_SASL_MECHANISM"),
		RedisURL:              getEnv("REDIS_URL"),
		LogLevel:              getEnv("LOG_LEVEL"),
		OTelEndpoint:          getEnv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		ConsumerGroup:         getEnv("CONSUMER_GROUP"),
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
