package config

import (
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
}

func LoadProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		Port:                  getEnv("PROCESSING_PORT", ":3000"),
		RedpandaBrokers:       strings.Split(getEnv("REDPANDA_BROKERS", "localhost:19092"), ","),
		RedpandaSASLUser:      getEnv("REDPANDA_SASL_USER", ""),
		RedpandaSASLPassword:  getEnv("REDPANDA_SASL_PASSWORD", ""),
		RedpandaSASLMechanism: getEnv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256"),
		RedisURL:              getEnv("REDIS_URL", "redis://localhost:6379"),
		LogLevel:              getEnv("LOG_LEVEL", "info"),
		OTelEndpoint:          getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
