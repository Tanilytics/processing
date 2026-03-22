package storage

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Tanilytics/processing/internal/models"
)

type ClickHouseWriter struct {
	conn clickhouse.Conn
}

func NewClickHouseWriter(addr, database, username, password string) (*ClickHouseWriter, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns: 10,
		MaxIdleConns: 5,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	return &ClickHouseWriter{conn: conn}, nil
}

func (w *ClickHouseWriter) WriteBatch(ctx context.Context, events []*models.ProcessedEvent) error {
	batch, err := w.conn.PrepareBatch(ctx, `
		INSERT INTO events (
			event_id, site_id, visitor_id, session_id, event_type, timestamp,
			url, referrer, utm_source, utm_medium, utm_campaign,
			country, region, device_type, browser, os, screen_width,
			properties, ip_hash, consent_given
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, e := range events {
		if err := batch.Append(
			e.EventID, e.SiteID, e.VisitorID, e.SessionID, string(e.EventType),
			e.Timestamp,
			e.URL, e.Referrer, e.UTMSource, e.UTMMedium, e.UTMCampaign,
			e.Country, e.Region, e.DeviceType, e.Browser, e.OS, e.ScreenWidth,
			string(e.Properties), e.IPHash, e.ConsentGiven,
		); err != nil {
			return fmt.Errorf("append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

func (w *ClickHouseWriter) Close() error {
	return w.conn.Close()
}
