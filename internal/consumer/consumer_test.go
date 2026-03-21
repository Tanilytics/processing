package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tanilytics/processing/internal/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBroker  = "localhost:9092"
	testGroupID = "test-group"
)

func TestNewEventConsumerWithoutSASL(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(brokers, testGroupID, "", "", "", &logger)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.NotNil(t, consumer.client)
	assert.NotNil(t, consumer.logger)
}

func TestNewEventConsumerWithSASLSHA256(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(brokers, testGroupID, "user", "pass", "SCRAM-SHA-256", &logger)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.NotNil(t, consumer.client)
}

func TestNewEventConsumerWithSASLSHA512(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(brokers, testGroupID, "user", "pass", "SCRAM-SHA-512", &logger)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.NotNil(t, consumer.client)
}

func TestNewEventConsumerWithEmptyMechanismFallsBackToSHA256(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(brokers, testGroupID, "user", "pass", "", &logger)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
}

func TestNewEventConsumerWithUnknownMechanismFallsBackToSHA256(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(brokers, testGroupID, "user", "pass", "UNKNOWN-MECH", &logger)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
}

func TestNewEventConsumerWithMultipleBrokers(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{"broker1:9092", "broker2:9092", "broker3:9092"}

	consumer, err := NewEventConsumer(brokers, testGroupID, "", "", "", &logger)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
}

func TestEventConsumerClose(t *testing.T) {
	logger := zerolog.Nop()
	brokers := []string{testBroker}

	consumer, err := NewEventConsumer(brokers, testGroupID, "", "", "", &logger)
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

	consumer, err := NewEventConsumer(brokers, testGroupID, "", "", "", &logger)
	require.NoError(t, err)
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = consumer.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}
