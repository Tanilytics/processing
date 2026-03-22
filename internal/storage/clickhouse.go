package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Tanilytics/processing/internal/models"
)

type Options struct {
	Addrs            []string
	Database         string
	Username         string
	Password         string
	DialTimeout      time.Duration
	MaxOpenConns     int
	MaxIdleConns     int
	ConnOpenStrategy string
}

type ClickHouseWriter struct {
	conn clickhouse.Conn
}

func NewClickHouseWriter(options Options) (*ClickHouseWriter, error) {
	strategy, err := parseConnOpenStrategy(options.ConnOpenStrategy)
	if err != nil {
		return nil, err
	}
	if len(options.Addrs) == 0 {
		return nil, fmt.Errorf("clickhouse addrs must not be empty")
	}
	if options.MaxOpenConns <= 0 {
		return nil, fmt.Errorf("clickhouse max open conns must be > 0")
	}
	if options.MaxIdleConns < 0 {
		return nil, fmt.Errorf("clickhouse max idle conns must be >= 0")
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: options.Addrs,
		Auth: clickhouse.Auth{
			Database: options.Database,
			Username: options.Username,
			Password: options.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:      options.DialTimeout,
		MaxOpenConns:     options.MaxOpenConns,
		MaxIdleConns:     options.MaxIdleConns,
		ConnOpenStrategy: strategy,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	return &ClickHouseWriter{conn: conn}, nil
}

func parseConnOpenStrategy(value string) (clickhouse.ConnOpenStrategy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "in_order":
		return clickhouse.ConnOpenInOrder, nil
	case "round_robin":
		return clickhouse.ConnOpenRoundRobin, nil
	case "random":
		return clickhouse.ConnOpenRandom, nil
	default:
		return 0, fmt.Errorf("invalid clickhouse conn open strategy %q: must be in_order, round_robin, or random", value)
	}
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
