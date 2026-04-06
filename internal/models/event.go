package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// EventType enumerates all valid event types.
type EventType string

const (
	EventPageView      EventType = "page_view"
	EventPageLeave     EventType = "page_leave"
	EventClick         EventType = "click"
	EventScroll        EventType = "scroll"
	EventMediaPlay     EventType = "media_play"
	EventMediaPause    EventType = "media_pause"
	EventMediaSeek     EventType = "media_seek"
	EventMediaProgress EventType = "media_progress"
	EventMediaBuffer   EventType = "media_buffer"
	EventMediaComplete EventType = "media_complete"
	EventFormSubmit    EventType = "form_submit"
)

// SessionContext holds device/browser context from the SDK.
type SessionContext struct {
	ScreenWidth  int    `json:"screen_width"`
	ScreenHeight int    `json:"screen_height"`
	Language     string `json:"language"`
	Timezone     string `json:"timezone"`
}

// InternalEvent is the enriched event produced to Redpanda.
// Contains server-side metadata not present in RawEvent.
type InternalEvent struct {
	EventID         string          `json:"event_id"`
	SiteID          string          `json:"site_id"`
	VisitorID       string          `json:"visitor_id"`
	EventType       EventType       `json:"event_type"`
	Timestamp       int64           `json:"timestamp"`
	URL             string          `json:"url"`
	Referrer        string          `json:"referrer,omitempty"`
	UTMSource       string          `json:"utm_source,omitempty"`
	UTMMedium       string          `json:"utm_medium,omitempty"`
	UTMCampaign     string          `json:"utm_campaign,omitempty"`
	SessionContext  SessionContext  `json:"session_context"`
	Properties      json.RawMessage `json:"properties,omitempty"`
	ServerTimestamp int64           `json:"server_timestamp"`
	IP              string          `json:"ip"`
	UserAgent       string          `json:"user_agent"`
}

// ProcessedEvent is the final event written to ClickHouse.
// IP and UserAgent are gone —> replaced by anonymized/parsed fields.
type ProcessedEvent struct {
	EventID      uuid.UUID
	SiteID       string
	VisitorID    string
	SessionID    string
	EventType    EventType
	Timestamp    time.Time
	URL          string
	Referrer     string
	UTMSource    string
	UTMMedium    string
	UTMCampaign  string
	Country      string
	Region       string
	DeviceType   string // desktop, mobile, tablet, bot
	Browser      string
	OS           string
	ScreenWidth  uint16
	Properties   json.RawMessage
	IPHash       string
	ConsentGiven bool
}
