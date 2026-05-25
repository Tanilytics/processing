package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Tanilytics/processing/internal/models"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

type realtimeUpdateMessage struct {
	Type        string `json:"type"`
	SiteID      string `json:"siteId"`
	EventCount  int    `json:"eventCount"`
	PublishedAt string `json:"publishedAt"`
}

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

func (r *RedisStore) UpdateCounters(ctx context.Context, events []*models.ProcessedEvent) error {
	pipe := r.client.Pipeline()
	now := time.Now().UTC()
	hourKey := now.Format("2006-01-02-15") // YYYY-MM-DD-HH
	siteUpdates := make(map[string]int)

	for _, e := range events {
		siteUpdates[e.SiteID]++

		if e.EventType == models.EventPageView {
			// Increment hourly page view counter
			pvKey := fmt.Sprintf("rt:pageviews:%s:%s", e.SiteID, hourKey)
			pipe.Incr(ctx, pvKey)
			pipe.Expire(ctx, pvKey, 25*time.Hour)

			// Add to HyperLogLog for unique visitors
			hllKey := fmt.Sprintf("rt:visitors:%s:%s", e.SiteID, hourKey)
			pipe.PFAdd(ctx, hllKey, e.VisitorID)
			pipe.Expire(ctx, hllKey, 25*time.Hour)
		}

		// Track active users (sorted set: visitor_id → timestamp)
		activeKey := fmt.Sprintf("rt:active:%s", e.SiteID)
		pipe.ZAdd(ctx, activeKey, redis.Z{
			Score:  float64(e.Timestamp.UnixMilli()),
			Member: e.VisitorID,
		})
		// Remove visitors inactive for > 5 minutes
		cutoff := float64(now.Add(-5 * time.Minute).UnixMilli())
		pipe.ZRemRangeByScore(ctx, activeKey, "-inf", fmt.Sprintf("%f", cutoff))
		pipe.Expire(ctx, activeKey, 10*time.Minute)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline: %w", err)
	}

	for siteID, eventCount := range siteUpdates {
		payload, err := json.Marshal(realtimeUpdateMessage{
			Type:        "realtime_counters_updated",
			SiteID:      siteID,
			EventCount:  eventCount,
			PublishedAt: now.Format(time.RFC3339Nano),
		})
		if err != nil {
			return fmt.Errorf("marshal realtime update: %w", err)
		}
		if err := r.client.Publish(ctx, fmt.Sprintf("rt:%s", siteID), string(payload)).Err(); err != nil {
			return fmt.Errorf("publish realtime update: %w", err)
		}
	}
	return nil
}
