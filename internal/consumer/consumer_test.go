package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Tanilytics/processing/internal/models"
	"github.com/Tanilytics/processing/internal/pipeline"
	"github.com/Tanilytics/processing/internal/processors"
	"github.com/Tanilytics/processing/internal/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	testBroker  = "localhost:9092"
	testGroupID = "test-group"
)

type fakeDLQProducer struct {
	produced []producedRecord
	err      error
}

type producedRecord struct {
	key     string
	value   []byte
	headers []kgo.RecordHeader
}

func (p *fakeDLQProducer) ProduceSync(
	_ context.Context,
	key string,
	value []byte,
	headers []kgo.RecordHeader,
) error {
	if p.err != nil {
		return p.err
	}

	p.produced = append(p.produced, producedRecord{
		key:     key,
		value:   append([]byte(nil), value...),
		headers: append([]kgo.RecordHeader(nil), headers...),
	})
	return nil
}

func TestNewEventConsumerWithoutSASL(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(brokers, ConnectionOptions{GroupID: testGroupID}, testOptions(), &fakeDLQProducer{}, testPipeline(), &logger)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.NotNil(t, consumer.client)
	assert.NotNil(t, consumer.pipeline)
	assert.NotNil(t, consumer.logger)
}

func TestNewEventConsumerWithSASLSHA256(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(
		brokers,
		ConnectionOptions{
			GroupID: testGroupID,
			SASL: SASLOptions{
				User:      "user",
				Password:  "pass",
				Mechanism: "SCRAM-SHA-256",
			},
		},
		testOptions(),
		&fakeDLQProducer{},
		testPipeline(),
		&logger,
	)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.NotNil(t, consumer.client)
}

func TestNewEventConsumerWithSASLSHA512(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(
		brokers,
		ConnectionOptions{
			GroupID: testGroupID,
			SASL: SASLOptions{
				User:      "user",
				Password:  "pass",
				Mechanism: "SCRAM-SHA-512",
			},
		},
		testOptions(),
		&fakeDLQProducer{},
		testPipeline(),
		&logger,
	)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.NotNil(t, consumer.client)
}

func TestNewEventConsumerWithEmptyMechanismFallsBackToSHA256(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(
		brokers,
		ConnectionOptions{
			GroupID: testGroupID,
			SASL: SASLOptions{
				User:     "user",
				Password: "pass",
			},
		},
		testOptions(),
		&fakeDLQProducer{},
		testPipeline(),
		&logger,
	)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
}

func TestNewEventConsumerWithUnknownMechanismFallsBackToSHA256(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(
		brokers,
		ConnectionOptions{
			GroupID: testGroupID,
			SASL: SASLOptions{
				User:      "user",
				Password:  "pass",
				Mechanism: "UNKNOWN-MECH",
			},
		},
		testOptions(),
		&fakeDLQProducer{},
		testPipeline(),
		&logger,
	)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
}

func TestNewEventConsumerWithMultipleBrokers(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{"broker1:9092", "broker2:9092", "broker3:9092"}

	consumer, err := NewEventConsumer(brokers, ConnectionOptions{GroupID: testGroupID}, testOptions(), &fakeDLQProducer{}, testPipeline(), &logger)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
}

func TestEventConsumerClose(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(brokers, ConnectionOptions{GroupID: testGroupID}, testOptions(), &fakeDLQProducer{}, testPipeline(), &logger)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		consumer.Close()
	})
}

func TestInternalEventUnmarshal(t *testing.T) {
	eventJSON := `{
		"event_id": "evt-123",
		"site_id": "site-456",
		"visitor_id": "vis-789",
		"event_type": "page_view",
		"timestamp": 1700000000000,
		"url": "https://example.com/page",
		"referrer": "https://google.com",
		"utm_source": "google",
		"utm_medium": "organic",
		"utm_campaign": "search",
		"session_context": {
			"screen_width": 1920,
			"screen_height": 1080,
			"language": "en-US",
			"timezone": "America/New_York"
		},
		"properties": {"key": "value"},
		"server_timestamp": 1700000001000,
		"ip": "192.168.1.1",
		"user_agent": "Mozilla/5.0"
	}`

	var event models.InternalEvent
	err := json.Unmarshal([]byte(eventJSON), &event)

	require.NoError(t, err)
	assert.Equal(t, "evt-123", event.EventID)
	assert.Equal(t, "site-456", event.SiteID)
	assert.Equal(t, "vis-789", event.VisitorID)
	assert.Equal(t, models.EventPageView, event.EventType)
	assert.Equal(t, int64(1700000000000), event.Timestamp)
	assert.Equal(t, "https://example.com/page", event.URL)
	assert.Equal(t, "https://google.com", event.Referrer)
	assert.Equal(t, "google", event.UTMSource)
	assert.Equal(t, "organic", event.UTMMedium)
	assert.Equal(t, "search", event.UTMCampaign)
	assert.Equal(t, 1920, event.SessionContext.ScreenWidth)
	assert.Equal(t, 1080, event.SessionContext.ScreenHeight)
	assert.Equal(t, "en-US", event.SessionContext.Language)
	assert.Equal(t, "America/New_York", event.SessionContext.Timezone)
	assert.Equal(t, int64(1700000001000), event.ServerTimestamp)
	assert.Equal(t, "192.168.1.1", event.IP)
	assert.Equal(t, "Mozilla/5.0", event.UserAgent)
}

func TestInternalEventUnmarshalMinimalEvent(t *testing.T) {
	eventJSON := `{
		"event_id": "evt-min",
		"site_id": "site-min",
		"visitor_id": "vis-min",
		"event_type": "click",
		"timestamp": 1700000000000,
		"url": "https://example.com"
	}`

	var event models.InternalEvent
	err := json.Unmarshal([]byte(eventJSON), &event)

	require.NoError(t, err)
	assert.Equal(t, "evt-min", event.EventID)
	assert.Equal(t, models.EventClick, event.EventType)
	assert.Empty(t, event.Referrer)
	assert.Empty(t, event.UTMSource)
	assert.Empty(t, event.IP)
}

func TestInternalEventUnmarshalInvalidJSON(t *testing.T) {
	invalidJSON := `{invalid json}`

	var event models.InternalEvent
	err := json.Unmarshal([]byte(invalidJSON), &event)

	assert.Error(t, err)
}

func TestInternalEventUnmarshalInvalidEventType(t *testing.T) {
	eventJSON := `{
		"event_id": "evt-123",
		"site_id": "site-456",
		"visitor_id": "vis-789",
		"event_type": "invalid_type",
		"timestamp": 1700000000000,
		"url": "https://example.com"
	}`

	var event models.InternalEvent
	err := json.Unmarshal([]byte(eventJSON), &event)

	require.NoError(t, err)
	assert.Equal(t, models.EventType("invalid_type"), event.EventType)
}

func TestRunContextCancellation(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(brokers, ConnectionOptions{GroupID: testGroupID}, testOptions(), &fakeDLQProducer{}, testPipeline(), &logger)
	require.NoError(t, err)
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = consumer.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestNewEventConsumerAppliesCustomOptions(t *testing.T) {
	logger := zerolog.Nop()
	opts := Options{
		Topic:                  "replay-events",
		ResetOffset:            "start",
		FetchMinBytes:          128 * 1024,
		FetchMaxBytes:          8 * 1024 * 1024,
		FetchMaxWait:           500 * time.Millisecond,
		FetchMaxPartitionBytes: 2 * 1024 * 1024,
		BlockRebalanceOnPoll:   true,
		MaxConcurrentFetches:   4,
		BatchSize:              100,
		BatchTimeout:           2 * time.Second,
	}

	consumer, err := NewEventConsumer([]string{testBroker}, ConnectionOptions{GroupID: testGroupID}, opts, &fakeDLQProducer{}, testPipeline(), &logger)
	require.NoError(t, err)
	defer consumer.Close()

	fetchMinBytes, ok := consumer.client.OptValue(kgo.FetchMinBytes).(int32)
	require.True(t, ok)
	assert.Equal(t, int32(128*1024), fetchMinBytes)

	fetchMaxBytes, ok := consumer.client.OptValue(kgo.FetchMaxBytes).(int32)
	require.True(t, ok)
	assert.Equal(t, int32(8*1024*1024), fetchMaxBytes)

	fetchMaxWait, ok := consumer.client.OptValue(kgo.FetchMaxWait).(time.Duration)
	require.True(t, ok)
	assert.Equal(t, 500*time.Millisecond, fetchMaxWait)

	fetchMaxPartitionBytes, ok := consumer.client.OptValue(kgo.FetchMaxPartitionBytes).(int32)
	require.True(t, ok)
	assert.Equal(t, int32(2*1024*1024), fetchMaxPartitionBytes)

	maxConcurrentFetches, ok := consumer.client.OptValue(kgo.MaxConcurrentFetches).(int)
	require.True(t, ok)
	assert.Equal(t, 4, maxConcurrentFetches)

	blockRebalanceOnPoll, ok := consumer.client.OptValue(kgo.BlockRebalanceOnPoll).(bool)
	require.True(t, ok)
	assert.True(t, blockRebalanceOnPoll)

	assert.Equal(t, 100, consumer.batchSize)
	assert.Equal(t, 2*time.Second, consumer.batchTimeout)
	assert.True(t, consumer.blockRebalanceOnPoll)
}

func TestParseResetOffset(t *testing.T) {
	start, err := parseResetOffset("start")
	require.NoError(t, err)
	assert.Equal(t, kgo.NewOffset().AtStart().String(), start.String())

	end, err := parseResetOffset("end")
	require.NoError(t, err)
	assert.Equal(t, kgo.NewOffset().AtEnd().String(), end.String())
}

func TestNewEventConsumerRejectsInvalidResetOffset(t *testing.T) {
	logger := zerolog.Nop()

	consumer, err := NewEventConsumer(
		[]string{testBroker},
		ConnectionOptions{GroupID: testGroupID},
		invalidResetOptions(),
		&fakeDLQProducer{},
		testPipeline(),
		&logger,
	)

	require.Error(t, err)
	assert.Nil(t, consumer)
	assert.ErrorContains(t, err, "must be start or end")
}

func TestNewEventConsumerRejectsNonPositiveFetchMaxBytes(t *testing.T) {
	logger := zerolog.Nop()
	opts := testOptions()
	opts.FetchMaxBytes = 0

	consumer, err := NewEventConsumer([]string{testBroker}, ConnectionOptions{GroupID: testGroupID}, opts, &fakeDLQProducer{}, testPipeline(), &logger)

	require.Error(t, err)
	assert.Nil(t, consumer)
	assert.ErrorContains(t, err, "fetch max bytes must be > 0")
}

func TestNewEventConsumerRejectsNonPositiveBatchSize(t *testing.T) {
	logger := zerolog.Nop()
	opts := testOptions()
	opts.BatchSize = 0

	consumer, err := NewEventConsumer([]string{testBroker}, ConnectionOptions{GroupID: testGroupID}, opts, &fakeDLQProducer{}, testPipeline(), &logger)

	require.Error(t, err)
	assert.Nil(t, consumer)
	assert.ErrorContains(t, err, "batch size must be > 0")
}

func TestNewEventConsumerRejectsNonPositiveBatchTimeout(t *testing.T) {
	logger := zerolog.Nop()
	opts := testOptions()
	opts.BatchTimeout = 0

	consumer, err := NewEventConsumer([]string{testBroker}, ConnectionOptions{GroupID: testGroupID}, opts, &fakeDLQProducer{}, testPipeline(), &logger)

	require.Error(t, err)
	assert.Nil(t, consumer)
	assert.ErrorContains(t, err, "batch timeout must be > 0")
}

func TestShouldFlushBatch(t *testing.T) {
	now := time.Now()

	assert.False(t, shouldFlushBatch(0, time.Time{}, 100, time.Second, now))
	assert.False(t, shouldFlushBatch(10, now, 100, time.Second, now.Add(500*time.Millisecond)))
	assert.True(t, shouldFlushBatch(100, now, 100, time.Second, now))
	assert.True(t, shouldFlushBatch(10, now, 100, time.Second, now.Add(time.Second)))
}

func TestHandleRecordRoutesMalformedJSONToDLQ(t *testing.T) {
	logger := zerolog.Nop()
	dlqProducer := &fakeDLQProducer{}
	consumer := &EventConsumer{dlqProducer: dlqProducer, logger: &logger}
	batch := &consumerBatch{}
	record := &kgo.Record{
		Topic:     "raw-events",
		Partition: 7,
		Offset:    11,
		Value:     []byte("{invalid json}"),
	}

	err := consumer.handleRecord(context.Background(), batch, record)

	require.NoError(t, err)
	require.Empty(t, batch.pending)
	require.Len(t, dlqProducer.produced, 1)
	assert.Equal(t, "raw-events:7:11", dlqProducer.produced[0].key)
	assert.Equal(t, []byte("{invalid json}"), dlqProducer.produced[0].value)
	assertHeaderValue(t, dlqProducer.produced[0].headers, "source_topic", "raw-events")
	assertHeaderValue(t, dlqProducer.produced[0].headers, "source_partition", "7")
	assertHeaderValue(t, dlqProducer.produced[0].headers, "source_offset", "11")
	assertHeaderValueContains(t, dlqProducer.produced[0].headers, "error_reason", "unmarshal event")
	assertHeaderValueContains(t, dlqProducer.produced[0].headers, "failed_at", "T")
}

func TestHandleRecordReturnsDLQError(t *testing.T) {
	logger := zerolog.Nop()
	dlqProducer := &fakeDLQProducer{err: errors.New("boom")}
	consumer := &EventConsumer{dlqProducer: dlqProducer, logger: &logger}
	batch := &consumerBatch{}
	record := &kgo.Record{Topic: "raw-events", Value: []byte("{invalid json}")}

	err := consumer.handleRecord(context.Background(), batch, record)

	require.Error(t, err)
	assert.ErrorContains(t, err, "route poison record to dlq")
	require.Empty(t, batch.pending)
	require.Empty(t, dlqProducer.produced)
}

func TestHandleRecordAppendsValidEvent(t *testing.T) {
	logger := zerolog.Nop()
	dlqProducer := &fakeDLQProducer{}
	consumer := &EventConsumer{dlqProducer: dlqProducer, logger: &logger}
	batch := &consumerBatch{}
	record := &kgo.Record{Value: []byte(`{"event_id":"evt-1","site_id":"site-1","visitor_id":"vis-1","event_type":"page_view","timestamp":1700000000000,"url":"https://example.com"}`)}

	err := consumer.handleRecord(context.Background(), batch, record)

	require.NoError(t, err)
	require.Len(t, batch.pending, 1)
	assert.Equal(t, "evt-1", batch.pending[0].EventID)
	assert.Empty(t, dlqProducer.produced)
}

func assertHeaderValue(t *testing.T, headers []kgo.RecordHeader, key, want string) {
	t.Helper()
	assert.Equal(t, want, headerValue(t, headers, key))
}

func assertHeaderValueContains(t *testing.T, headers []kgo.RecordHeader, key, want string) {
	t.Helper()
	assert.Contains(t, headerValue(t, headers, key), want)
}

func headerValue(t *testing.T, headers []kgo.RecordHeader, key string) string {
	t.Helper()
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}

	t.Fatalf("missing header %q", key)
	return ""
}

func testOptions() Options {
	return Options{
		Topic:                  "raw-events",
		ResetOffset:            "end",
		FetchMinBytes:          64 * 1024,
		FetchMaxBytes:          50 * 1024 * 1024,
		FetchMaxWait:           250 * time.Millisecond,
		FetchMaxPartitionBytes: 4 * 1024 * 1024,
		MaxConcurrentFetches:   2,
		BatchSize:              1000,
		BatchTimeout:           time.Second,
	}
}

func invalidResetOptions() Options {
	opts := testOptions()
	opts.ResetOffset = "middle"
	return opts
}

func testPipeline() *pipeline.Pipeline {
	writer, err := storage.NewClickHouseWriter(storage.Options{
		Addrs:            []string{"some-addr"},
		Database:         "default",
		Username:         "default",
		Password:         "test-password",
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnOpenStrategy: "in_order",
	})
	if err != nil {
		panic(err)
	}
	logger := zerolog.Nop()
	return pipeline.NewPipeline(
		processors.NewAnonymizer("test-salt"),
		processors.NewUserAgentParser(),
		writer,
		&logger,
	)
}
