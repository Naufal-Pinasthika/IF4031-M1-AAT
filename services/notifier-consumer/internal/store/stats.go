package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"go.etcd.io/bbolt"
)

var statsKey = []byte("stats_v1")
var statsReadyKey = []byte("stats_v1_ready")
var statsCursorKey = []byte("stats_v1_cursor")
var latencyBuckets = [][]byte{[]byte("latency_pipeline_v1"), []byte("latency_source_v1"), []byte("latency_coastal_v1")}

// Exact nearest-rank quantiles, maintained with delivery in the same transaction.
// Adding one sample moves the rank by at most one sorted value. Scrapes read
// one small summary, never the notification history or an in-memory sample list.
type quantile struct {
	Total uint64
	Below uint64
	Value int64
}

type statsState struct {
	Stats
	Quantiles [3]quantile
}

func readStats(tx *bbolt.Tx) (statsState, error) {
	var state statsState
	err := json.Unmarshal(tx.Bucket(metadataBucket).Get(statsKey), &state)
	return state, err
}

func (s statsState) save(tx *bbolt.Tx) error {
	payload, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return tx.Bucket(metadataBucket).Put(statsKey, payload)
}

func (q *quantile) add(bucket *bbolt.Bucket, value int64) error {
	if value < 0 {
		value = 0
	}
	key := encodeCount(uint64(value))
	if err := bucket.Put(key, encodeCount(decodeCount(bucket.Get(key))+1)); err != nil {
		return err
	}
	if q.Total == 0 {
		q.Value = value
	}
	q.Total++
	if value < q.Value {
		q.Below++
	}
	rank := q.Total - q.Total/20 // ceil(0.95 * N), without overflow
	cursor := bucket.Cursor()
	key, count := cursor.Seek(encodeCount(uint64(q.Value)))
	for rank <= q.Below {
		key, count = cursor.Prev()
		if key == nil {
			return fmt.Errorf("invalid latency rank")
		}
		q.Below -= decodeCount(count)
	}
	for rank > q.Below+decodeCount(count) {
		q.Below += decodeCount(count)
		key, count = cursor.Next()
		if key == nil {
			return fmt.Errorf("invalid latency rank")
		}
	}
	q.Value = int64(decodeCount(key))
	return nil
}

func (s *statsState) add(tx *bbolt.Tx, n Notification) error {
	s.NotificationCount++
	if err := s.Quantiles[0].add(tx.Bucket(latencyBuckets[0]), n.PipelineLatencyMS); err != nil {
		return err
	}
	if n.SourceLatencyMS != nil {
		if err := s.Quantiles[1].add(tx.Bucket(latencyBuckets[1]), *n.SourceLatencyMS); err != nil {
			return err
		}
	}
	if n.Critical {
		s.CriticalCount++
		if n.Source == "SENSOR_PANTAI" {
			s.CoastalCriticalCount++
			if err := s.Quantiles[2].add(tx.Bucket(latencyBuckets[2]), n.IngressLatencyMS); err != nil {
				return err
			}
		}
	}
	s.P95PipelineLatencyMS = s.Quantiles[0].Value
	s.P95SourceLatencyMS = s.Quantiles[1].Value
	s.P95CoastalCriticalIngressMS = s.Quantiles[2].Value
	return nil
}

// Upgrade legacy databases once, in restartable batches before Kafka starts.
// A batch's summary, index and cursor commit together; no notifications are
// deleted, re-delivered or loaded as an unbounded slice.
func (s *Store) initializeStats() error {
	started := time.Now()
	var processed int
	for {
		done := false
		err := s.db.Update(func(tx *bbolt.Tx) error {
			metadata := tx.Bucket(metadataBucket)
			if metadata.Get(statsReadyKey) != nil {
				state, err := readStats(tx)
				if err != nil {
					return err
				}
				if state.ProcessedCount == int(decodeCount(metadata.Get(processedKey))) {
					done = true
					return nil
				}
				// An older binary may have delivered rows after a rollback.
				// Rebuild only derived indexes/summary, never delivery/dedup data.
				for _, name := range latencyBuckets {
					if err := tx.DeleteBucket(name); err != nil {
						return err
					}
				}
				for _, key := range [][]byte{statsKey, statsCursorKey, statsReadyKey} {
					if err := metadata.Delete(key); err != nil {
						return err
					}
				}
			}
			for _, name := range latencyBuckets {
				if _, err := tx.CreateBucketIfNotExists(name); err != nil {
					return err
				}
			}
			var state statsState
			if metadata.Get(statsKey) != nil {
				var err error
				state, err = readStats(tx)
				if err != nil {
					return err
				}
			}
			state.ProcessedCount = int(decodeCount(metadata.Get(processedKey)))
			cursor := tx.Bucket(notificationsBucket).Cursor()
			key, value := cursor.First()
			if after := metadata.Get(statsCursorKey); after != nil {
				key, value = cursor.Seek(after)
				if bytes.Equal(key, after) {
					key, value = cursor.Next()
				}
			}
			var last []byte
			for batch := 0; key != nil && batch < 4096; batch++ {
				var n Notification
				if err := json.Unmarshal(value, &n); err != nil {
					return err
				}
				if err := state.add(tx, n); err != nil {
					return err
				}
				last = append(last[:0], key...)
				key, value = cursor.Next()
			}
			if last != nil {
				if err := metadata.Put(statsCursorKey, last); err != nil {
					return err
				}
			}
			if err := state.save(tx); err != nil {
				return err
			}
			processed = state.NotificationCount
			if key == nil {
				done = true
				return metadata.Put(statsReadyKey, []byte{1})
			}
			return nil
		})
		if processed > 0 {
			slog.Info("notifier_statistics_upgrade", "correlation_id", "startup", "notifications", processed, "complete", done, "elapsed_ms", time.Since(started).Milliseconds(), "error", err)
		}
		if err != nil || done {
			return err
		}
	}
}
