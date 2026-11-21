package store

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"go.etcd.io/bbolt"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
)

var (
	notificationsBucket = []byte("notifications")
	processedBucket     = []byte("processed_messages")
	metadataBucket      = []byte("metadata")
	processedKey        = []byte("processed_count")
)

type Store struct {
	db *bbolt.DB
}

var ErrInvalidPage = errors.New("invalid notification page")

type NotificationPage struct {
	Notifications []Notification `json:"notifications"`
	NextCursor    string         `json:"next_cursor,omitempty"`
}

// ListNotifications bounds both the read transaction and response allocation.
// The cursor follows the persistent domain key, not a mutable list offset.
// Concurrent inserts before the cursor may require a fresh traversal; this is
// a live view, not a snapshot spanning multiple HTTP requests.
func (s *Store) ListNotifications(ctx context.Context, limit int, rawCursor string) (NotificationPage, error) {
	page := NotificationPage{Notifications: make([]Notification, 0)}
	if limit < 1 || limit > 1000 || len(rawCursor) > 4096 {
		return page, ErrInvalidPage
	}
	var after []byte
	if rawCursor != "" {
		var err error
		after, err = base64.RawURLEncoding.DecodeString(rawCursor)
		if err != nil || len(after) == 0 {
			return page, ErrInvalidPage
		}
	}
	err := s.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(notificationsBucket).Cursor()
		key, value := cursor.First()
		if len(after) > 0 {
			key, value = cursor.Seek(after)
			if string(key) == string(after) {
				key, value = cursor.Next()
			}
		}
		var last []byte
		for key != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(page.Notifications) == limit {
				page.NextCursor = base64.RawURLEncoding.EncodeToString(last)
				break
			}
			var item Notification
			if err := json.Unmarshal(value, &item); err != nil {
				return err
			}
			page.Notifications = append(page.Notifications, item)
			last = append(last[:0], key...)
			key, value = cursor.Next()
		}
		return ctx.Err()
	})
	return page, err
}

type Notification struct {
	MessageID         string    `json:"message_id"`
	HazardID          string    `json:"hazard_id"`
	Source            string    `json:"source"`
	HazardType        string    `json:"hazard_type"`
	Severity          string    `json:"severity"`
	AreaName          string    `json:"area_name"`
	Message           string    `json:"message"`
	CorrelationID     string    `json:"correlation_id"`
	CreatedAt         time.Time `json:"created_at"`
	PipelineLatencyMS int64     `json:"pipeline_latency_ms"`
	SourceLatencyMS   *int64    `json:"source_latency_ms"`
	IngressLatencyMS  int64     `json:"ingress_latency_ms"`
	Critical          bool      `json:"critical"`
}

type Stats struct {
	NotificationCount           int   `json:"notification_count"`
	ProcessedCount              int   `json:"processed_count"`
	CriticalCount               int   `json:"critical_count"`
	CoastalCriticalCount        int   `json:"coastal_critical_count"`
	P95PipelineLatencyMS        int64 `json:"p95_pipeline_latency_ms"`
	P95SourceLatencyMS          int64 `json:"p95_source_latency_ms"`
	P95CoastalCriticalIngressMS int64 `json:"p95_coastal_critical_ingress_ms"`
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create notifier data directory: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Second, InitialMmapSize: 1 << 30})
	if err != nil {
		return nil, fmt.Errorf("open notifier store: %w", err)
	}
	result := &Store{db: db}
	if err := db.Update(func(tx *bbolt.Tx) error {
		for _, bucket := range [][]byte{notificationsBucket, processedBucket, metadataBucket} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize notifier store: %w", err)
	}
	if err := result.initializeStats(); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize notifier statistics: %w", err)
	}
	return result, nil
}

func (s *Store) Deliver(messageID string, envelope event.Envelope) (bool, error) {
	if messageID == "" {
		return false, fmt.Errorf("message ID is required")
	}
	if envelope.EventType == "hazard.event.deleted" {
		return false, nil
	}
	createdAt := time.Now().UTC()
	notification := notificationFor(messageID, envelope, createdAt)
	payload, err := json.Marshal(notification)
	if err != nil {
		return false, fmt.Errorf("encode notification: %w", err)
	}

	delivered := false
	err = s.db.Update(func(tx *bbolt.Tx) error {
		processed := tx.Bucket(processedBucket)
		if processed.Get([]byte(messageID)) != nil {
			return nil
		}
		notifications := tx.Bucket(notificationsBucket)
		domainKey := []byte(envelope.EventType + ":" + envelope.Data.HazardID + ":" + envelope.Data.Severity)
		if notifications.Get(domainKey) == nil {
			if err := notifications.Put(domainKey, payload); err != nil {
				return err
			}
			delivered = true
		}
		if err := processed.Put([]byte(messageID), []byte(notification.CreatedAt.Format(time.RFC3339Nano))); err != nil {
			return err
		}
		metadata := tx.Bucket(metadataBucket)
		count := decodeCount(metadata.Get(processedKey)) + 1
		if err := metadata.Put(processedKey, encodeCount(count)); err != nil {
			return err
		}
		state, err := readStats(tx)
		if err != nil {
			return err
		}
		state.ProcessedCount = int(count)
		if delivered {
			if err := state.add(tx, notification); err != nil {
				return err
			}
		}
		return state.save(tx)
	})
	return delivered, err
}

func notificationFor(messageID string, envelope event.Envelope, createdAt time.Time) Notification {
	var sourceLatency *int64
	if envelope.Data.Source != "SENSOR_PANTAI" {
		elapsed := nonNegativeMilliseconds(createdAt.Sub(envelope.Data.OccurredAt))
		sourceLatency = &elapsed
	}
	var ingressLatency int64
	if !envelope.Data.IngestedAt.IsZero() {
		ingressLatency = nonNegativeMilliseconds(createdAt.Sub(envelope.Data.IngestedAt))
	}
	critical, _ := envelope.Data.Attributes["critical"].(bool)
	return Notification{
		MessageID:         messageID,
		HazardID:          envelope.Data.HazardID,
		Source:            envelope.Data.Source,
		HazardType:        envelope.Data.HazardType,
		Severity:          envelope.Data.Severity,
		AreaName:          envelope.Data.AreaName,
		Message:           fmt.Sprintf("%s: %s at %s", envelope.Data.Severity, envelope.Data.HazardType, envelope.Data.AreaName),
		CorrelationID:     envelope.CorrelationID,
		CreatedAt:         createdAt,
		PipelineLatencyMS: nonNegativeMilliseconds(createdAt.Sub(envelope.PublishedAt)),
		SourceLatencyMS:   sourceLatency,
		IngressLatencyMS:  ingressLatency,
		Critical:          critical,
	}
}

func (s *Store) Notifications() ([]Notification, error) {
	items := make([]Notification, 0)
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(notificationsBucket).ForEach(func(_, value []byte) error {
			var item Notification
			if err := json.Unmarshal(value, &item); err != nil {
				return err
			}
			items = append(items, item)
			return nil
		})
	})
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	return items, err
}

func (s *Store) Stats() (Stats, error) {
	var result Stats
	err := s.db.View(func(tx *bbolt.Tx) error {
		state, err := readStats(tx)
		result = state.Stats
		return err
	})
	return result, err
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.View(func(*bbolt.Tx) error { return ctx.Err() })
}

func encodeCount(value uint64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, value)
	return buffer
}

func decodeCount(value []byte) uint64 {
	if len(value) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}

func nonNegativeMilliseconds(duration time.Duration) int64 {
	if duration < 0 {
		return 0
	}
	return duration.Milliseconds()
}
