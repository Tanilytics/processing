package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/Tanilytics/processing/internal/models"
	"github.com/colinmarc/hdfs/v2"
	"github.com/google/uuid"
	local "github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type HDFSOptions struct {
	NameNodeAddr string
	User         string
	BasePath     string
}

type HDFSWriter struct {
	client   *hdfs.Client
	basePath string
}

func NewHDFSWriter(options HDFSOptions) (*HDFSWriter, error) {
	if options.NameNodeAddr == "" {
		return nil, fmt.Errorf("hdfs namenode address must not be empty")
	}
	if options.BasePath == "" {
		return nil, fmt.Errorf("hdfs base path must not be empty")
	}

	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{options.NameNodeAddr},
		User:      options.User,
	})
	if err != nil {
		return nil, fmt.Errorf("hdfs connect: %w", err)
	}

	return &HDFSWriter{
		client:   client,
		basePath: options.BasePath,
	}, nil
}

func (w *HDFSWriter) WriteBatch(ctx context.Context, events []*models.ProcessedEvent) error {
	if len(events) == 0 {
		return nil
	}

	now := time.Now().UTC()
	dir := path.Join(w.basePath,
		fmt.Sprintf("%d", now.Year()),
		fmt.Sprintf("%02d", now.Month()),
		fmt.Sprintf("%02d", now.Day()),
		fmt.Sprintf("%02d", now.Hour()),
	)

	if err := w.client.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("hdfs mkdir %s: %w", dir, err)
	}

	fileName := path.Join(dir, fmt.Sprintf("events-%s.parquet", uuid.New().String()))

	// Write parquet to a temporary local file first, then copy to HDFS.
	tmpFile, err := os.CreateTemp("", "events-*.parquet")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpName)

	fw, err := local.NewLocalFileWriter(tmpName)
	if err != nil {
		return fmt.Errorf("parquet local writer: %w", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(parquetEvent), 4)
	if err != nil {
		_ = fw.Close()
		return fmt.Errorf("parquet writer: %w", err)
	}

	for _, e := range events {
		pe := toParquetEvent(e)
		if err := pw.Write(pe); err != nil {
			_ = pw.WriteStop()
			_ = fw.Close()
			return fmt.Errorf("parquet write: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		_ = fw.Close()
		return fmt.Errorf("parquet write stop: %w", err)
	}
	if err := fw.Close(); err != nil {
		return fmt.Errorf("parquet file close: %w", err)
	}

	localFile, err := os.Open(tmpName)
	if err != nil {
		return fmt.Errorf("open temp file: %w", err)
	}
	defer localFile.Close()

	hdfsFile, err := w.client.Create(fileName)
	if err != nil {
		return fmt.Errorf("hdfs create %s: %w", fileName, err)
	}
	defer hdfsFile.Close()

	if _, err := io.Copy(hdfsFile, localFile); err != nil {
		return fmt.Errorf("hdfs copy %s: %w", fileName, err)
	}

	return nil
}

func (w *HDFSWriter) Close() error {
	return w.client.Close()
}

type parquetEvent struct {
	EventID      string `parquet:"name=event_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SiteID       string `parquet:"name=site_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	VisitorID    string `parquet:"name=visitor_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SessionID    string `parquet:"name=session_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	EventType    string `parquet:"name=event_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	EventName    string `parquet:"name=event_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Timestamp    int64  `parquet:"name=timestamp, type=INT64"`
	URL          string `parquet:"name=url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Referrer     string `parquet:"name=referrer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	UTMSource    string `parquet:"name=utm_source, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	UTMMedium    string `parquet:"name=utm_medium, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	UTMCampaign  string `parquet:"name=utm_campaign, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Country      string `parquet:"name=country, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Region       string `parquet:"name=region, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceType   string `parquet:"name=device_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Browser      string `parquet:"name=browser, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OS           string `parquet:"name=os, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ScreenWidth  int32  `parquet:"name=screen_width, type=INT32"`
	Properties   string `parquet:"name=properties, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	IPHash       string `parquet:"name=ip_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ConsentGiven bool   `parquet:"name=consent_given, type=BOOLEAN"`
}

func toParquetEvent(e *models.ProcessedEvent) *parquetEvent {
	return &parquetEvent{
		EventID:      e.EventID.String(),
		SiteID:       e.SiteID,
		VisitorID:    e.VisitorID,
		SessionID:    e.SessionID,
		EventType:    string(e.EventType),
		EventName:    e.EventName,
		Timestamp:    e.Timestamp.UnixMilli(),
		URL:          e.URL,
		Referrer:     e.Referrer,
		UTMSource:    e.UTMSource,
		UTMMedium:    e.UTMMedium,
		UTMCampaign:  e.UTMCampaign,
		Country:      e.Country,
		Region:       e.Region,
		DeviceType:   e.DeviceType,
		Browser:      e.Browser,
		OS:           e.OS,
		ScreenWidth:  int32(e.ScreenWidth),
		Properties:   string(e.Properties),
		IPHash:       e.IPHash,
		ConsentGiven: e.ConsentGiven,
	}
}
