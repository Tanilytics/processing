package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Tanilytics/processing/internal/models"
	redismock "github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisStoreUpdateCountersForPageView(t *testing.T) {
	client, mock := redismock.NewClientMock()
	store := NewRedisStore(client)

	now := time.Now().UTC()
	hourKey := now.Format("2006-01-02-15")
	eventTS := now.Add(-time.Minute)
	event := &models.ProcessedEvent{
		SiteID:    "site-1",
		VisitorID: "visitor-1",
		EventType: models.EventPageView,
		Timestamp: eventTS,
	}

	pvKey := fmt.Sprintf("rt:pageviews:%s:%s", event.SiteID, hourKey)
	hllKey := fmt.Sprintf("rt:visitors:%s:%s", event.SiteID, hourKey)
	activeKey := fmt.Sprintf("rt:active:%s", event.SiteID)

	mock.ExpectIncr(pvKey).SetVal(1)
	mock.ExpectExpire(pvKey, 25*time.Hour).SetVal(true)
	mock.ExpectPFAdd(hllKey, event.VisitorID).SetVal(1)
	mock.ExpectExpire(hllKey, 25*time.Hour).SetVal(true)
	mock.ExpectZAdd(activeKey, redis.Z{Score: float64(eventTS.UnixMilli()), Member: event.VisitorID}).SetVal(1)
	mock.Regexp().ExpectZRemRangeByScore(activeKey, "-inf", `^[0-9]+\.000000$`).SetVal(0)
	mock.ExpectExpire(activeKey, 10*time.Minute).SetVal(true)

	err := store.UpdateCounters(context.Background(), []*models.ProcessedEvent{event})
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRedisStoreUpdateCountersSkipsPageViewCountersForOtherEvents(t *testing.T) {
	client, mock := redismock.NewClientMock()
	store := NewRedisStore(client)

	now := time.Now().UTC()
	eventTS := now.Add(-time.Minute)
	event := &models.ProcessedEvent{
		SiteID:    "site-1",
		VisitorID: "visitor-2",
		EventType: models.EventClick,
		Timestamp: eventTS,
	}

	activeKey := fmt.Sprintf("rt:active:%s", event.SiteID)

	mock.ExpectZAdd(activeKey, redis.Z{Score: float64(eventTS.UnixMilli()), Member: event.VisitorID}).SetVal(1)
	mock.Regexp().ExpectZRemRangeByScore(activeKey, "-inf", `^[0-9]+\.000000$`).SetVal(0)
	mock.ExpectExpire(activeKey, 10*time.Minute).SetVal(true)

	err := store.UpdateCounters(context.Background(), []*models.ProcessedEvent{event})
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}
