package processors

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestGetOrCreateSessionReturnsGeneratedIDWhenManagerIsNil(t *testing.T) {
	var manager *SessionManager

	sessionID, err := manager.GetOrCreateSession(context.Background(), "site-1", "visitor-1", 1700000000000)
	if err != nil {
		t.Fatalf("GetOrCreateSession error = %v", err)
	}
	if sessionID == "" {
		t.Fatal("expected session ID to be set")
	}
	if _, err := uuid.Parse(sessionID); err != nil {
		t.Fatalf("session ID %q is not a valid UUID: %v", sessionID, err)
	}
}

func TestGetOrCreateSessionReturnsGeneratedIDWhenRedisClientIsNil(t *testing.T) {
	manager := NewSessionManager(nil)

	sessionID, err := manager.GetOrCreateSession(context.Background(), "site-1", "visitor-1", 1700000000000)
	if err != nil {
		t.Fatalf("GetOrCreateSession error = %v", err)
	}
	if sessionID == "" {
		t.Fatal("expected session ID to be set")
	}
	if _, err := uuid.Parse(sessionID); err != nil {
		t.Fatalf("session ID %q is not a valid UUID: %v", sessionID, err)
	}
}

func TestSessionKey(t *testing.T) {
	got := sessionKey("site-1", "visitor-1")
	if got != "session:site-1:visitor-1" {
		t.Fatalf("sessionKey() = %q, want %q", got, "session:site-1:visitor-1")
	}
}
