package producer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

const testBrokerAddr = "localhost:69"

func testProducerOptions() Options {
	return Options{
		Topic:              "dlq",
		BatchMaxBytes:      1024 * 1024,
		Linger:             10 * time.Millisecond,
		MaxBufferedRecords: 50000,
		RecordRetries:      3,
		RetryTimeout:       30 * time.Second,
	}
}

func TestNewRedpandaProducerNoSASL(t *testing.T) {
	p, err := NewRedpandaProducer(
		[]string{testBrokerAddr},
		ConnectionOptions{},
		testProducerOptions(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, p)
	defer p.Close()
}

func TestNewRedpandaProducerWithSASLSHA256(t *testing.T) {
	p, err := NewRedpandaProducer(
		[]string{testBrokerAddr},
		ConnectionOptions{
			SASL: SASLOptions{
				User:      "user",
				Password:  "pass",
				Mechanism: "SCRAM-SHA-256",
			},
		},
		testProducerOptions(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, p)
	defer p.Close()
}

func TestNewRedpandaProducerWithSASLSHA512(t *testing.T) {
	p, err := NewRedpandaProducer(
		[]string{testBrokerAddr},
		ConnectionOptions{
			SASL: SASLOptions{
				User:      "user",
				Password:  "pass",
				Mechanism: "SCRAM-SHA-512",
			},
		},
		testProducerOptions(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, p)
	defer p.Close()
}

func TestNewRedpandaProducerUnknownMechanismFallsBackToSHA256(t *testing.T) {
	p, err := NewRedpandaProducer(
		[]string{testBrokerAddr},
		ConnectionOptions{
			SASL: SASLOptions{
				User:      "user",
				Password:  "pass",
				Mechanism: "UNKNOWN",
			},
		},
		testProducerOptions(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, p)
	defer p.Close()
}

func TestCloneHeadersClonesHeaderValues(t *testing.T) {
	value := []byte("bad payload")
	headers := []kgo.RecordHeader{{Key: "error_reason", Value: value}}

	cloned := cloneHeaders(headers)
	value[0] = 'g'

	require.Equal(t, []byte("bad payload"), cloned[0].Value)
	require.NotEqual(t, &headers[0].Value[0], &cloned[0].Value[0])
}
