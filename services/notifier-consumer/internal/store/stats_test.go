package store

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"go.etcd.io/bbolt"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
)

func TestIncrementalQuantileMatchesSortedReference(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "quantiles.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	random := rand.New(rand.NewSource(72))
	for _, order := range []string{"random", "ascending", "descending", "duplicates"} {
		t.Run(order, func(t *testing.T) {
			err := s.db.Update(func(tx *bbolt.Tx) error {
				name := []byte("test_" + order)
				bucket, err := tx.CreateBucket(name)
				if err != nil {
					return err
				}
				var q quantile
				var values []int64
				for i := 0; i < 2500; i++ {
					value := random.Int63n(1000000)
					switch order {
					case "ascending":
						value = int64(i)
					case "descending":
						value = int64(2500 - i)
					case "duplicates":
						value = int64(i % 7)
					}
					values = append(values, value)
					if err := q.add(bucket, value); err != nil {
						return err
					}
					want := append([]int64(nil), values...)
					sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
					if q.Value != want[(95*len(want)+99)/100-1] {
						t.Fatalf("sample %d: got %d", i, q.Value)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLegacyStatsUpgradePreservesHistoryAndDeduplication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, bucket := range [][]byte{notificationsBucket, processedBucket, metadataBucket} {
			if _, err := tx.CreateBucket(bucket); err != nil {
				return err
			}
		}
		for i := 0; i < 1500; i++ {
			n := Notification{HazardID: fmt.Sprint(i), PipelineLatencyMS: int64(i), Source: "SENSOR_PANTAI", Critical: true, IngressLatencyMS: int64(i)}
			payload, _ := json.Marshal(n)
			if err := tx.Bucket(notificationsBucket).Put([]byte(event.UpsertedType+":"+n.HazardID+":AWAS"), payload); err != nil {
				return err
			}
		}
		if err := tx.Bucket(processedBucket).Put([]byte("existing"), []byte("yes")); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(processedKey, encodeCount(1600))
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	want := Stats{NotificationCount: 1500, ProcessedCount: 1600, CriticalCount: 1500, CoastalCriticalCount: 1500, P95PipelineLatencyMS: 1424, P95CoastalCriticalIngressMS: 1424}
	got, err := s.Stats()
	if err != nil || got != want {
		t.Fatalf("upgrade: %+v %v", got, err)
	}
	envelope := event.Envelope{EventType: event.UpsertedType, Data: event.HazardEvent{HazardID: "0", Severity: "AWAS"}}
	if delivered, err := s.Deliver("existing", envelope); delivered || err != nil {
		t.Fatalf("message dedup: %v %v", delivered, err)
	}
	if delivered, err := s.Deliver("new-offset-same-event", envelope); delivered || err != nil {
		t.Fatalf("domain dedup: %v %v", delivered, err)
	}
	want.ProcessedCount++
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got, err := s.Stats(); err != nil || got != want {
		t.Fatalf("restart: %+v %v", got, err)
	}
	// Scraping must not decode historical rows or run Bucket.Stats().
	if err := s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(notificationsBucket).Put([]byte("zzz"), []byte("invalid JSON"))
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Stats(); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("scrape scanned history: %+v %v", got, err)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if _, err := s.Stats(); err != nil {
			panic(err)
		}
	}); allocations > 40 {
		t.Fatalf("scrape allocated %.0f objects", allocations)
	}
}

func TestStatsUpgradeResumesCommittedBatchAndDetectsOlderWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resume.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	err = s.db.Update(func(tx *bbolt.Tx) error {
		metadata := tx.Bucket(metadataBucket)
		if err := metadata.Delete(statsReadyKey); err != nil {
			return err
		}
		var partial statsState
		for i := 0; i < 10; i++ {
			n := Notification{HazardID: fmt.Sprintf("%02d", i), PipelineLatencyMS: int64(i)}
			payload, _ := json.Marshal(n)
			if err := tx.Bucket(notificationsBucket).Put([]byte(n.HazardID), payload); err != nil {
				return err
			}
			if i < 4 {
				if err := partial.add(tx, n); err != nil {
					return err
				}
			}
		}
		if err := metadata.Put(processedKey, encodeCount(10)); err != nil {
			return err
		}
		partial.ProcessedCount = 10
		if err := partial.save(tx); err != nil {
			return err
		}
		return metadata.Put(statsCursorKey, []byte("03"))
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	want := Stats{NotificationCount: 10, ProcessedCount: 10, P95PipelineLatencyMS: 9}
	if got, err := s.Stats(); err != nil || got != want {
		t.Fatalf("resume: %+v %v", got, err)
	}
	err = s.db.Update(func(tx *bbolt.Tx) error {
		payload, _ := json.Marshal(Notification{HazardID: "old-writer", PipelineLatencyMS: 50})
		if err := tx.Bucket(notificationsBucket).Put([]byte("old-writer"), payload); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(processedKey, encodeCount(11))
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	want.NotificationCount = 11
	want.ProcessedCount = 11
	want.P95PipelineLatencyMS = 50
	if got, err := s.Stats(); err != nil || got != want {
		t.Fatalf("older writer: %+v %v", got, err)
	}
}
