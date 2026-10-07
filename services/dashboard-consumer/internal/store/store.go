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

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/dashboard-consumer/internal/event"
)

var (
	eventsBucket    = []byte("events")
	processedBucket = []byte("processed_messages")
	metadataBucket  = []byte("metadata")
	processedKey    = []byte("processed_count")
)

type Store struct {
	db *bbolt.DB
}

type Stats struct {
	EventCount     int `json:"event_count"`
	ProcessedCount int `json:"processed_count"`
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create dashboard data directory: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open dashboard store: %w", err)
	}
	result := &Store{db: db}
	if err := db.Update(func(tx *bbolt.Tx) error {
		for _, bucket := range [][]byte{eventsBucket, processedBucket, metadataBucket} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize dashboard store: %w", err)
	}
	return result, nil
}

func (s *Store) Apply(messageID string, envelope event.Envelope) (bool, error) {
	if messageID == "" {
		return false, fmt.Errorf("message ID is required")
	}
	payload, err := json.Marshal(envelope.Data)
	if err != nil {
		return false, fmt.Errorf("encode dashboard event: %w", err)
	}

	applied := false
	err = s.db.Update(func(tx *bbolt.Tx) error {
		processed := tx.Bucket(processedBucket)
		if processed.Get([]byte(messageID)) != nil {
			return nil
		}
		if err := tx.Bucket(eventsBucket).Put([]byte(envelope.Data.HazardID), payload); err != nil {
			return err
		}
		if err := processed.Put([]byte(messageID), []byte(time.Now().UTC().Format(time.RFC3339Nano))); err != nil {
			return err
		}
		metadata := tx.Bucket(metadataBucket)
		count := decodeCount(metadata.Get(processedKey)) + 1
		if err := metadata.Put(processedKey, encodeCount(count)); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

func (s *Store) Events() ([]event.HazardEvent, error) {
	items := make([]event.HazardEvent, 0)
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(eventsBucket).ForEach(func(_, value []byte) error {
			var item event.HazardEvent
			if err := json.Unmarshal(value, &item); err != nil {
				return err
			}
			items = append(items, item)
			return nil
		})
	})
	sort.Slice(items, func(i, j int) bool { return items[i].OccurredAt.After(items[j].OccurredAt) })
	return items, err
}

func (s *Store) Stats() (Stats, error) {
	var result Stats
	err := s.db.View(func(tx *bbolt.Tx) error {
		result.EventCount = tx.Bucket(eventsBucket).Stats().KeyN
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
