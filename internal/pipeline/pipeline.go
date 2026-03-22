package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/Tanilytics/processing/internal/models"
	"github.com/Tanilytics/processing/internal/processors"
	"github.com/Tanilytics/processing/internal/storage"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Pipeline struct {
	anonymizer *processors.Anonymizer
	uaParser   *processors.UserAgentParser
	chWriter   *storage.ClickHouseWriter
	logger     *zerolog.Logger
}

func NewPipeline(
	anonymizer *processors.Anonymizer,
	uaParser *processors.UserAgentParser,
	chWriter *storage.ClickHouseWriter,
	logger *zerolog.Logger,
) *Pipeline {
	return &Pipeline{
		anonymizer: anonymizer,
		uaParser:   uaParser,
		chWriter:   chWriter,
		logger:     logger,
	}
}

func (p *Pipeline) Process(ctx context.Context, events []*models.InternalEvent) error {
	processed := make([]*models.ProcessedEvent, 0, len(events))

	for _, raw := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pe, err := p.processEvent(raw)
		if err != nil {
			return err
		}
		processed = append(processed, pe)
	}

	// Step 4: Batch write to ClickHouse
	if err := p.chWriter.WriteBatch(ctx, processed); err != nil {
		return fmt.Errorf("clickhouse write: %w", err)
	}

	return nil
}

func (p *Pipeline) processEvent(raw *models.InternalEvent) (*models.ProcessedEvent, error) {
	// Step 1: Anonymize IP
	ipHash, country, region := p.anonymizer.Anonymize(raw.IP)

	// Step 2: Parse User-Agent
	browser, os, deviceType := p.uaParser.Parse(raw.UserAgent)

	// Step 3: Parse eventID (in case it fails it derives a uuid instead of failing)
	eventID, derived := parseUUID(raw.EventID)
	if derived {
		p.logger.Debug().Str("event_id", raw.EventID).Str("derived_uuid", eventID.String()).Msg("derived uuid from event id")
	}

	return &models.ProcessedEvent{
		EventID:      eventID,
		SiteID:       raw.SiteID,
		VisitorID:    raw.VisitorID,
		SessionID:    "unknown",
		EventType:    raw.EventType,
		Timestamp:    time.UnixMilli(raw.Timestamp),
		URL:          raw.URL,
		Referrer:     raw.Referrer,
		UTMSource:    raw.UTMSource,
		UTMMedium:    raw.UTMMedium,
		UTMCampaign:  raw.UTMCampaign,
		Country:      country,
		Region:       region,
		DeviceType:   deviceType,
		Browser:      browser,
		OS:           os,
		ScreenWidth:  clampUint16(raw.SessionContext.ScreenWidth),
		Properties:   raw.Properties,
		IPHash:       ipHash,
		ConsentGiven: false,
	}, nil
}

func parseUUID(raw string) (uuid.UUID, bool) {
	parsed, err := uuid.Parse(raw)
	if err == nil {
		return parsed, false
	}

	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(raw)), true
}

func clampUint16(value int) uint16 {
	const maxUint16 = 1<<16 - 1

	if value <= 0 {
		return 0
	}
	if value > maxUint16 {
		return maxUint16
	}

	return uint16(value)
}
