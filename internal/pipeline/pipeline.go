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
	sessionMgr *processors.SessionManager
	hdfsWriter *storage.HDFSWriter
	redisStore *storage.RedisStore
	logger     *zerolog.Logger
}

func NewPipeline(
	anonymizer *processors.Anonymizer,
	uaParser *processors.UserAgentParser,
	sessionMgr *processors.SessionManager,
	hdfsWriter *storage.HDFSWriter,
	redisStore *storage.RedisStore,
	logger *zerolog.Logger,
) *Pipeline {
	return &Pipeline{
		anonymizer: anonymizer,
		uaParser:   uaParser,
		sessionMgr: sessionMgr,
		hdfsWriter: hdfsWriter,
		redisStore: redisStore,
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

		pe, err := p.processEvent(ctx, raw)
		if err != nil {
			return err
		}
		processed = append(processed, pe)
	}

	// Step 5: Batch write to HDFS
	if err := p.hdfsWriter.WriteBatch(ctx, processed); err != nil {
		return fmt.Errorf("hdfs write: %w", err)
	}

	// Step 6: Update Redis counters
	if err := p.redisStore.UpdateCounters(ctx, processed); err != nil {
		// Redis counter failure is non-fatal —> log but don't fail the pipeline
		p.logger.Error().Err(err).Msg("redis counter update failed")
	}

	return nil
}

func (p *Pipeline) processEvent(ctx context.Context, raw *models.InternalEvent) (*models.ProcessedEvent, error) {
	// Step 1: Anonymize IP
	ipHash, country, region := p.anonymizer.Anonymize(raw.IP)

	// Step 2: Parse User-Agent
	browser, os, deviceType := p.uaParser.Parse(raw.UserAgent)

	// Step 3: Parse eventID (in case it fails it derives a uuid instead of failing)
	eventID, derived := parseUUID(raw.EventID)
	if derived {
		p.logger.Debug().Str("event_id", raw.EventID).Str("derived_uuid", eventID.String()).Msg("derived uuid from event id")
	}

	// Step 4: Session stitching
	sessionID, err := p.sessionMgr.GetOrCreateSession(ctx, raw.SiteID, raw.VisitorID, raw.Timestamp)
	if err != nil {
		p.logger.Error().
			Err(err).
			Str("site_id", raw.SiteID).
			Str("visitor_id", raw.VisitorID).
			Msg("session stitching failed")
		// Use a fallback session ID so we don't lose the event
		sessionID = "unknown"
	}

	return &models.ProcessedEvent{
		EventID:      eventID,
		SiteID:       raw.SiteID,
		VisitorID:    raw.VisitorID,
		SessionID:    sessionID,
		EventType:    raw.EventType,
		EventName:    raw.EventName,
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
