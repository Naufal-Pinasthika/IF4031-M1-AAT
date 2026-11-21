package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

func analyticalOracle(events []model.HazardEvent, from, to *time.Time) Analytics {
	r := Analytics{ByHazard: hazardCounts(), BySeverity: map[string]int{"NORMAL": 0, "WASPADA": 0, "SIAGA": 0, "AWAS": 0}, ByArea: map[string]int{}, Daily: []DailyCounts{}, ReadStatus: ReadStatus{UnavailableUnits: []string{}}}
	days := map[string]map[string]int{}
	for _, e := range events {
		if removed, _ := e.Attributes["_retracted"].(bool); removed || from != nil && e.OccuredAt.Before(*from) || to != nil && e.OccuredAt.After(*to) {
			continue
		}
		r.Total++
		r.ByHazard[string(e.HazardType)]++
		r.BySeverity[string(e.Severity)]++
		r.ByArea[e.AreaName]++
		day := e.OccuredAt.UTC().Format("2006-01-02")
		if days[day] == nil {
			days[day] = hazardCounts()
		}
		days[day][string(e.HazardType)]++
	}
	for day, counts := range days {
		r.Daily = append(r.Daily, DailyCounts{Date: day, Counts: counts})
	}
	sort.Slice(r.Daily, func(i, j int) bool { return r.Daily[i].Date < r.Daily[j].Date })
	return r
}

func assertAnalyticalCounts(t *testing.T, got, want Analytics) {
	t.Helper()
	got.ReadStatus, want.ReadStatus = ReadStatus{}, ReadStatus{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("analytical counts differ from independent latest-event oracle:\n got=%+v\nwant=%+v", got, want)
	}
}

func TestPostgresAnalyticsReconcilesVersionsBeforeFiltersAndRetractions(t *testing.T) {
	s, _, settings := integrationReplicas(t)
	var latest []model.HazardEvent
	kinds := []model.HazardType{model.HazardTypeSEISMIC, model.HazardTypeTSUNAMI, model.HazardTypeVOLCANIC, model.HazardTypeWILDFIRE, model.HazardTypeCOASTAL}
	for i := range 80 {
		e := testHazard()
		e.SourceRefID = fmt.Sprintf("analytics-version-%03d", i)
		e.HazardType = kinds[i%len(kinds)]
		e.AreaName = fmt.Sprintf("area-%d", i%4)
		e.OccuredAt = time.Date(2025, 1, 1+i%9, 0, 30, 0, 0, time.FixedZone("UTC+9", 9*3600))
		e.IngestedAt = time.Now().UTC()
		if i%7 == 0 {
			e.Attributes = map[string]any{"_retracted": true}
		} else if i%13 == 0 {
			e.Attributes = map[string]any{"_retracted": "true"}
		}
		pair := s.destinations(partitionFor(string(e.Source), e.SourceRefID))
		if err := s.UpsertAcceptedTarget(t.Context(), &e, int64(i+100), "canonical:"+settings[pair[i%2]].Name); err != nil {
			t.Fatal(err)
		}
		latest = append(latest, e)
		if i%3 != 0 {
			old := e
			old.Severity, old.AreaName = model.SeverityAWAS, "stale-area"
			old.OccuredAt = old.OccuredAt.Add(30 * 24 * time.Hour)
			old.Attributes = map[string]any{}
			if err := s.UpsertAcceptedTarget(t.Context(), &old, int64(i+1), "canonical:"+settings[pair[1-i%2]].Name); err != nil {
				t.Fatal(err)
			}
		}
	}
	from, to := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 6, 23, 59, 59, 0, time.UTC)
	for _, bounds := range [][2]*time.Time{{nil, nil}, {&from, &to}, {&from, nil}, {nil, &to}, {&to, &to}} {
		got, err := s.Analytics(t.Context(), bounds[0], bounds[1])
		if err != nil || got.Partial || !got.ReplicationDegraded {
			t.Fatalf("unexpected latest-replica analytical status: %+v %v", got.ReadStatus, err)
		}
		assertAnalyticalCounts(t, got, analyticalOracle(latest, bounds[0], bounds[1]))
	}
}

func TestPostgresAnalyticsPreservesExplicitPartialAndCallerCancellation(t *testing.T) {
	s, _, _ := integrationReplicas(t)
	var events []model.HazardEvent
	for i := range 40 {
		e := testHazard()
		e.SourceRefID = fmt.Sprintf("analytics-availability-%03d", i)
		e.IngestedAt = time.Now().UTC()
		if err := s.UpsertAccepted(t.Context(), &e, int64(i+1)); err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	s.markDown(s.units[0])
	got, err := s.Analytics(t.Context(), nil, nil)
	if err != nil || !got.Partial || !reflect.DeepEqual(got.UnavailableUnits, []string{s.units[0].name}) {
		t.Fatalf("missing unit must remain visibly partial: %+v %v", got.ReadStatus, err)
	}
	assertAnalyticalCounts(t, got, analyticalOracle(events, nil, nil))
	s.markDown(s.units[1])
	var available []model.HazardEvent
	for _, e := range events {
		p := partitionFor(string(e.Source), e.SourceRefID)
		if p == 1 || p == 2 {
			available = append(available, e)
		}
	}
	got, err = s.Analytics(t.Context(), nil, nil)
	if err != nil || !got.Partial || len(got.UnavailableUnits) != 2 {
		t.Fatal(got.ReadStatus, err)
	}
	assertAnalyticalCounts(t, got, analyticalOracle(available, nil, nil))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Analytics(ctx, nil, nil); err == nil {
		t.Fatal("canceled analytical request was reported successful")
	}
}

func TestPostgresAnalyticsLargePayloadFitsExistingGatewayBudget(t *testing.T) {
	s, databases, _ := integrationReplicas(t)
	const count = 120000
	for i, db := range databases {
		tx, err := db.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(t.Context(), "SET LOCAL statement_timeout='30s'"); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(t.Context(), `INSERT INTO hazard_events
 (hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,partition_id,revision,ingestion_version)
 SELECT 'analytics-'||lpad(n::text,9,'0'),'BMKG','large-'||n,'SEISMIC','NORMAL','test-area',-1,120,
 '2025-01-01'::timestamptz + (n%31)*interval '1 day','2025-02-01'::timestamptz,
 jsonb_build_object('practice',true,'payload',repeat('x',8192)),n%3,n,n
 FROM generate_series(1,$1::int) n WHERE n%3=$2 OR (n%3+1)%3=$2`, count, i)
		if err != nil {
			tx.Rollback(t.Context())
			t.Fatal(err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	s.timeout = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	started := time.Now()
	got, err := s.Analytics(ctx, nil, nil)
	cancel()
	if err != nil || got.Partial || got.Total != count || got.ByHazard["SEISMIC"] != count || got.BySeverity["NORMAL"] != count || got.ByArea["test-area"] != count || len(got.Daily) != 31 {
		t.Fatalf("large analytical scan failed within unchanged gateway budget: total=%d partial=%v elapsed=%v error=%v", got.Total, got.Partial, time.Since(started), err)
	}
	t.Logf("exactly %d logical records with 8 KiB attributes; analytical latency=%v", got.Total, time.Since(started))
	interrupted, stop := context.WithTimeout(t.Context(), 10*time.Millisecond)
	if _, err := s.Analytics(interrupted, nil, nil); err == nil {
		t.Fatal("interrupted analytical scan was reported successful")
	}
	stop()
	for _, u := range s.units {
		if len(u.readSlots) != 0 {
			t.Fatal("canceled analytics leaked a storage read admission slot")
		}
	}
	planTx, err := databases[0].pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer planTx.Rollback(t.Context())
	if _, err := planTx.Exec(t.Context(), "SET LOCAL cursor_tuple_fraction=0.001; SET LOCAL enable_sort=off"); err != nil {
		t.Fatal(err)
	}
	rows, err := planTx.Query(t.Context(), "EXPLAIN (COSTS OFF) DECLARE analytical_plan CURSOR FOR "+analyticsQuery, 0)
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(&plan, line)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("cursor query plan (captured after latency measurement):\n%s", plan.String())
}

func TestPostgresAnalyticsRejectsSaturationAndReleasesReadSlots(t *testing.T) {
	s, _, _ := integrationReplicas(t)
	s.timeout = 30 * time.Millisecond
	for _, u := range s.units {
		for len(u.readSlots) < cap(u.readSlots) {
			u.readSlots <- struct{}{}
		}
	}
	_, err := s.Analytics(t.Context(), nil, nil)
	for _, u := range s.units {
		for len(u.readSlots) > 0 {
			<-u.readSlots
		}
	}
	if !errors.Is(err, ErrStorageBusy) {
		t.Fatalf("saturated storage must reject, not report complete empty counts: %v", err)
	}
	s.timeout = 500 * time.Millisecond
	got, err := s.Analytics(t.Context(), nil, nil)
	if err != nil || got.Partial || got.Total != 0 {
		t.Fatalf("storage did not recover after admission saturation: %+v %v", got, err)
	}
}
