package store

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

// Differential oracle: a separate, complete PostgreSQL dataset has ONLY the
// newest version of each identity. Replica placement, stale severity/time,
// filters, ties, offsets and pagination vary; a certified complete response
// must equal the oracle, not merely return HTTP 200 or have partial=false.
func TestPostgresReplicaCoverageMatchesIndependentLatestDataset(t *testing.T) {
	s, _, settings := integrationReplicas(t)
	_, oracleURL := integrationCoordinator(t)
	oracle, err := New(t.Context(), []config.StorageUnit{{Name: "oracle", DatabaseURL: oracleURL}}, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer oracle.Close()
	latest := map[string]model.HazardEvent{}
	for i := range 70 {
		e := testHazard()
		e.SourceRefID = fmt.Sprintf("coverage-%03d", i)
		e.OccuredAt = e.OccuredAt.Add(time.Duration(i/2) * time.Minute) // deliberate ties
		e.IngestedAt = e.OccuredAt.Add(time.Second)
		if i%3 == 0 {
			e.Severity = model.SeverityAWAS
		}
		version := int64(10 * (i + 1))
		if err := oracle.UpsertAccepted(t.Context(), &e, version); err != nil {
			t.Fatal(err)
		}
		latest[e.HazardID] = e
		pair := s.destinations(partitionFor(string(e.Source), e.SourceRefID))
		if err := s.UpsertAcceptedTarget(t.Context(), &e, version, "canonical:"+settings[pair[i%2]].Name); err != nil {
			t.Fatal(err)
		}
		if i%4 != 0 {
			continue
		} // omitted peer, or a stale mutable-field version
		old := e
		old.Severity = model.SeverityAWAS
		if e.Severity == model.SeverityAWAS {
			old.Severity = model.SeverityWASPADA
		}
		old.OccuredAt = old.OccuredAt.Add(200 * time.Minute)
		old.IngestedAt = old.IngestedAt.Add(-200 * time.Minute)
		if err := s.UpsertAcceptedTarget(t.Context(), &old, version-1, "canonical:"+settings[pair[1-i%2]].Name); err != nil {
			t.Fatal(err)
		}
	}
	from := testHazard().OccuredAt.Add(8 * time.Minute)
	to := from.Add(12 * time.Minute)
	filters := []EventFilter{{}, {Severity: "AWAS"}, {Severity: "WASPADA"}, {From: &from, To: &to}, {Audit: true}, {Audit: true, Since: &from}, {Source: "BMKG", Severity: "AWAS", From: &from}}
	completeChecks := 0
	for _, filter := range filters {
		for _, limit := range []int{1, 3, 7, 20, 50} {
			for _, offset := range []int{0, 5} {
				f := filter
				f.Limit = limit
				f.Offset = offset
				expected, err := oracle.ListEvents(t.Context(), f)
				if err != nil {
					t.Fatal(err)
				}
				got, err := s.ListEvents(t.Context(), f)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range got.Events {
					if !reflect.DeepEqual(row, latest[row.HazardID]) {
						t.Fatalf("stale candidate escaped reconciliation: %+v", row)
					}
				}
				if !got.Partial {
					completeChecks++
					if !reflect.DeepEqual(got.Events, expected.Events) || !reflect.DeepEqual(got.NextCursor, expected.NextCursor) {
						t.Fatalf("false complete page (audit=%v severity=%s limit=%d offset=%d): got=%v expected=%v", f.Audit, f.Severity, limit, offset, got, expected)
					}
				}
			}
		}
	}
	if completeChecks < 60 {
		t.Fatalf("coverage proof was vacuous: only %d/70 complete checks", completeChecks)
	}
	t.Logf("oracle-verified complete comparisons: %d/70; every returned event checked against latest version", completeChecks)
}

func TestPostgresCoverageNeverCertifiesUnavailableReplica(t *testing.T) {
	s, databases, _ := integrationReplicas(t)
	// Retain diagnostics on fixture failures without changing its 100ms budget,
	// locking scenario or assertions.
	s.logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	e := testHazard()
	e.IngestedAt = time.Now().UTC()
	if err := s.UpsertAccepted(t.Context(), &e, 10); err != nil {
		t.Fatal(err)
	}
	pair := s.destinations(partitionFor(string(e.Source), e.SourceRefID))
	lock, err := databases[pair[0]].pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(t.Context(), `LOCK TABLE hazard_events IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	s.timeout = 100 * time.Millisecond
	got, err := s.GetEventByID(t.Context(), e.HazardID)
	if err != nil || got.Event == nil || !got.Partial || len(got.UnavailableUnits) == 0 {
		t.Fatalf("unavailable replica was relabelled complete: %+v %v", got, err)
	}
	missing, err := s.GetEventByID(t.Context(), "nonexistent-identity")
	if err != nil || missing.Event != nil || !missing.Partial {
		t.Fatalf("absence was falsely certified while a replica was unavailable: %+v %v", missing, err)
	}
}

func TestPostgresAuditRefillsLargeStalePrefix(t *testing.T) {
	s, databases, _ := integrationReplicas(t)
	// One peer orders a stale version first; its latest version belongs at the
	// tail. More than 4,000 rows keep every bounded refill non-exhausted.
	for _, index := range s.destinations(0) {
		_, err := databases[index].pool.Exec(t.Context(), `INSERT INTO hazard_events
		 (hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,
		 occurred_at,ingested_at,attributes,partition_id,revision,ingestion_version)
		 SELECT 'audit-'||lpad(i::text,6,'0'),'BMKG','audit-'||i,'SEISMIC','WASPADA','Padang',0,0,
		 '2026-01-01'::timestamptz+i*interval '1 second',
		 '2026-01-01'::timestamptz+i*interval '1 second','{}',0,1,1
		 FROM generate_series(0,5999) AS i`)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := databases[s.destinations(0)[0]].pool.Exec(t.Context(), `UPDATE hazard_events
	 SET ingested_at='2026-01-02',revision=2,ingestion_version=2 WHERE hazard_id='audit-000000'`)
	if err != nil {
		t.Fatal(err)
	}
	page, err := s.ListEvents(t.Context(), EventFilter{Audit: true, Limit: 1000})
	if err != nil || page.Partial || !page.ReplicationDegraded || len(page.Events) != 1000 || page.NextCursor == nil {
		t.Fatalf("large Audit prefix was not certified after bounded refill: count=%d status=%+v err=%v", len(page.Events), page.ReadStatus, err)
	}
	for i, event := range page.Events {
		if event.HazardID != fmt.Sprintf("audit-%06d", i+1) {
			t.Fatalf("stale prefix changed pagination at %d: %s", i, event.HazardID)
		}
	}
}
