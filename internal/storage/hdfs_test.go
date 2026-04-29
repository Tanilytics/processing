package storage

import (
	"testing"
	"time"

	"github.com/Tanilytics/processing/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToParquetEvent(t *testing.T) {
	ts := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	e := &models.ProcessedEvent{
		EventID:      uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
		SiteID:       "site-1",
		VisitorID:    "visitor-1",
		SessionID:    "session-1",
		EventType:    models.EventPageView,
		EventName:    "home",
		Timestamp:    ts,
		URL:          "https://example.com",
		Referrer:     "https://google.com",
		UTMSource:    "google",
		UTMMedium:    "cpc",
		UTMCampaign:  "spring",
		Country:      "US",
		Region:       "CA",
		DeviceType:   "desktop",
		Browser:      "Chrome",
		OS:           "macOS",
		ScreenWidth:  1920,
		Properties:   []byte(`{"key":"value"}`),
		IPHash:       "abc123",
		ConsentGiven: true,
	}

	pe := toParquetEvent(e)
	assert.Equal(t, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", pe.EventID)
	assert.Equal(t, "site-1", pe.SiteID)
	assert.Equal(t, ts.UnixMilli(), pe.Timestamp)
	assert.Equal(t, int32(1920), pe.ScreenWidth)
	assert.Equal(t, `{"key":"value"}`, pe.Properties)
	assert.True(t, pe.ConsentGiven)
}

func TestNewHDFSWriterRejectsEmptyAddr(t *testing.T) {
	writer, err := NewHDFSWriter(HDFSOptions{
		NameNodeAddr: "",
		BasePath:     "/analytics",
	})
	require.Error(t, err)
	assert.Nil(t, writer)
	assert.ErrorContains(t, err, "namenode address must not be empty")
}

func TestNewHDFSWriterRejectsEmptyBasePath(t *testing.T) {
	writer, err := NewHDFSWriter(HDFSOptions{
		NameNodeAddr: "namenode:9000",
		BasePath:     "",
	})
	require.Error(t, err)
	assert.Nil(t, writer)
	assert.ErrorContains(t, err, "base path must not be empty")
}
