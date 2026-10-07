package store

import (
	"encoding/binary"
	"encoding/json"
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

type Notification struct {
	MessageID     string    `json:"message_id"`
	HazardID      string    `json:"hazard_id"`
	Source        string    `json:"source"`
	HazardType    string    `json:"hazard_type"`
	Severity      string    `json:"severity"`
	AreaName      string    `json:"area_name"`
	Message       string    `json:"message"`
	CorrelationID string    `json:"correlation_id"`
	CreatedAt     time.Time `json:"created_at"`
}

type Stats struct {
	NotificationCount int `json:"notification_count"`
	ProcessedCount    int `json:"processed_count"`
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create notifier data directory: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
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
	return result, nil
}

func (s *Store) Deliver(messageID string, envelope event.Envelope) (bool, error) {
	if messageID == "" {
		return false, fmt.Errorf("message ID is required")
	}
	notification := Notification{
		MessageID:     messageID,
		HazardID:      envelope.Data.HazardID,
		Source:        envelope.Data.Source,
		HazardType:    envelope.Data.HazardType,
		Severity:      envelope.Data.Severity,
		AreaName:      envelope.Data.AreaName,
		Message:       fmt.Sprintf("%s: %s at %s", envelope.Data.Severity, envelope.Data.HazardType, envelope.Data.AreaName),
		CorrelationID: envelope.CorrelationID,
		CreatedAt:     time.Now().UTC(),
	}
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
		return nil
	})
	return delivered, err
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
		result.NotificationCount = tx.Bucket(notificationsBucket).Stats().KeyN
		result.ProcessedCount = int(decodeCount(tx.Bucket(metadataBucket).Get(processedKey)))
		return nil
	})
	return result, err
}

func (s *Store) Close() error { return s.db.Close() }

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
