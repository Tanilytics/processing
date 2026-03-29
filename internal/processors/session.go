package processors

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const sessionTimeout = 30 * time.Minute

type SessionManager struct {
	redis *redis.Client
}

func NewSessionManager(redis *redis.Client) *SessionManager {
	return &SessionManager{redis: redis}
}

// sessionKey returns the Redis key for a visitor's active session.
func sessionKey(siteID, visitorID string) string {
	return fmt.Sprintf("session:%s:%s", siteID, visitorID)
}

type sessionState struct {
	SessionID   string `json:"session_id"`
	LastEventTS int64  `json:"last_event_ts"`
	PageCount   int    `json:"page_count"`
}

// GetOrCreateSession returns an existing session_id if the visitor has been active
// within the last 30 minutes, or creates a new session.
func (s *SessionManager) GetOrCreateSession(ctx context.Context, siteID, visitorID string, eventTS int64) (string, error) {
	if s == nil || s.redis == nil {
		return uuid.New().String(), nil
	}

	key := sessionKey(siteID, visitorID)

	// Get current session state
	data, err := s.redis.Get(ctx, key).Bytes()
	if err == redis.Nil {
		// No active session → create new
		return s.createSession(ctx, key, eventTS)
	}
	if err != nil {
		return "", fmt.Errorf("redis get session: %w", err)
	}

	var state sessionState
	if err := json.Unmarshal(data, &state); err != nil {
		// Corrupted state → create new session
		return s.createSession(ctx, key, eventTS)
	}

	// Check if session has timed out
	lastEvent := time.UnixMilli(state.LastEventTS)
	currentEvent := time.UnixMilli(eventTS)
	if currentEvent.Sub(lastEvent) > sessionTimeout {
		// Session expired → create new
		return s.createSession(ctx, key, eventTS)
	}

	// Update existing session
	state.LastEventTS = eventTS
	state.PageCount++
	updated, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("marshal session state: %w", err)
	}
	if err := s.redis.Set(ctx, key, updated, sessionTimeout).Err(); err != nil {
		return "", fmt.Errorf("redis set session: %w", err)
	}

	return state.SessionID, nil
}

func (s *SessionManager) createSession(ctx context.Context, key string, eventTS int64) (string, error) {
	sessionID := uuid.New().String()
	state := sessionState{
		SessionID:   sessionID,
		LastEventTS: eventTS,
		PageCount:   1,
	}
	data, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("marshal session state: %w", err)
	}
	if err := s.redis.Set(ctx, key, data, sessionTimeout).Err(); err != nil {
		return "", fmt.Errorf("redis set session: %w", err)
	}
	return sessionID, nil
}
