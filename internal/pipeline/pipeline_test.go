package pipeline

import (
	"testing"

	"github.com/Tanilytics/processing/internal/models"
	"github.com/Tanilytics/processing/internal/processors"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

func TestParseUUIDKeepsValidUUID(t *testing.T) {
	expected := uuid.New()

	parsed, derived := parseUUID(expected.String())
	if derived {
		t.Fatal("expected valid UUID to be preserved")
	}
	if parsed != expected {
		t.Fatalf("parsed = %s, want %s", parsed, expected)
	}
}

func TestParseUUIDDerivesDeterministicUUID(t *testing.T) {
	first, firstDerived := parseUUID("evt-123")
	second, secondDerived := parseUUID("evt-123")

	if !firstDerived || !secondDerived {
		t.Fatal("expected invalid event IDs to derive UUIDs")
	}
	if first == uuid.Nil {
		t.Fatal("expected derived UUID to be non-nil")
	}
	if first != second {
		t.Fatalf("derived UUIDs differ: %s vs %s", first, second)
	}
}

func TestProcessEventDerivesUUIDAndClampsScreenWidth(t *testing.T) {
	logger := zerolog.Nop()
	p := NewPipeline(
		processors.NewAnonymizer("test-salt"),
		processors.NewUserAgentParser(),
		&logger,
	)

	processed, err := p.processEvent(&models.InternalEvent{
		EventID:   "evt-123",
		SiteID:    "site-1",
		VisitorID: "visitor-1",
		EventType: models.EventPageView,
		Timestamp: 1700000000000,
		URL:       "https://example.com",
		IP:        "192.168.1.12",
		UserAgent: "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
		SessionContext: models.SessionContext{
			ScreenWidth: 100000,
		},
	})
	if err != nil {
		t.Fatalf("processEvent error = %v", err)
	}

	expectedID, _ := parseUUID("evt-123")
	if processed.EventID != expectedID {
		t.Fatalf("EventID = %s, want %s", processed.EventID, expectedID)
	}
	if processed.ScreenWidth != 65535 {
		t.Fatalf("ScreenWidth = %d, want 65535", processed.ScreenWidth)
	}
	if processed.DeviceType != "mobile" {
		t.Fatalf("DeviceType = %q, want %q", processed.DeviceType, "mobile")
	}
	if processed.Browser != "Safari" {
		t.Fatalf("Browser = %q, want %q", processed.Browser, "Safari")
	}
	if processed.OS != "iOS" {
		t.Fatalf("OS = %q, want %q", processed.OS, "iOS")
	}
	if processed.IPHash == "" {
		t.Fatal("expected IPHash to be set")
	}
}

func TestClampUint16(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  uint16
	}{
		{name: "negative", input: -1, want: 0},
		{name: "zero", input: 0, want: 0},
		{name: "fits", input: 1024, want: 1024},
		{name: "too large", input: 70000, want: 65535},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clampUint16(tt.input)
			if got != tt.want {
				t.Fatalf("clampUint16(%d) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}
