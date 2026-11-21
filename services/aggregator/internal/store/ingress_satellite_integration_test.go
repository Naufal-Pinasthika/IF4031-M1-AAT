package store

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

func satellitePayload(satellite string, lat float64, frp float64) []byte {
	raw, _ := json.Marshal(map[string]any{"satellite": satellite, "acq_date": "2026-11-20", "acq_time": 100, "latitude": lat, "longitude": 113.0, "confidence": "h", "frp": frp})
	return raw
}

func TestM2SatelliteAtomicReplayRetractAndRestart(t *testing.T) {
	for round := 0; round < 5; round++ {
		c, url := integrationCoordinator(t)
		ctx := t.Context()
		lease, won, err := c.AcquireOrRenewLease(ctx, "SATELIT", "worker", time.Minute)
		if err != nil || !won {
			t.Fatal(err)
		}
		bootstrap := bytes.Replace(satellitePayload("BOOT", -2, 0), []byte(`"confidence":"h"`), []byte(`"confidence":"l"`), 1)
		if err = c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", bootstrap, 0); err != nil {
			t.Fatal("first real feed offset is zero", err)
		}
		a, b := satellitePayload("N20", -2, 20), satellitePayload("N21", -2.001, 20)
		if err = c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", a, 1); err != nil {
			t.Fatal(err)
		}
		if err = c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", b, 2); err != nil {
			t.Fatal(err)
		}
		var count int
		if err = c.pool.QueryRow(ctx, `SELECT count(*) FROM satellite_events`).Scan(&count); err != nil || count != 1 {
			t.Fatal(count, err)
		}
		var ref string
		_ = c.pool.QueryRow(ctx, `SELECT source_ref_id FROM satellite_events`).Scan(&ref)
		reopened, err := NewCoordinator(ctx, url, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		if offset, err := reopened.SatelliteOffset(ctx); err != nil || offset != 2 {
			t.Fatal(offset, err)
		}
		if err = reopened.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", b, 2); err != nil {
			t.Fatal(err)
		}
		var intents int
		_ = c.pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_outbox WHERE source='SATELIT'`).Scan(&intents)
		if intents != 1 {
			t.Fatalf("replay duplicate intents: %d", intents)
		}
		update := []byte(`{"key":"N20|2026-11-20|100|-2|113","changes":{"frp":0}}`)
		if err = c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "update", update, 3); err != nil {
			t.Fatal(err)
		}
		var frp float64
		_ = c.pool.QueryRow(ctx, `SELECT (attributes->>'total_frp_mw')::float FROM satellite_events`).Scan(&frp)
		if frp != 20 {
			t.Fatal("zero FRP ignored", frp)
		}
		var after string
		_ = c.pool.QueryRow(ctx, `SELECT source_ref_id FROM satellite_events`).Scan(&after)
		if after != ref {
			t.Fatal("update changed fire identity")
		}
		if err = c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "retract", []byte(`{"detection_key":"N21|2026-11-20|100|-2.001|113"}`), 4); err != nil {
			t.Fatal(err)
		}
		_ = c.pool.QueryRow(ctx, `SELECT count(*) FROM satellite_events`).Scan(&count)
		if count != 0 {
			t.Fatal("retraction retained fire")
		}
		var removed bool
		_ = c.pool.QueryRow(ctx, `SELECT (payload->'attributes'->>'_retracted')::boolean FROM ingestion_outbox ORDER BY version DESC LIMIT 1`).Scan(&removed)
		if !removed {
			t.Fatal("missing durable tombstone")
		}
		if err = c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", a, 6); err == nil {
			t.Fatal("offset gap accepted")
		}
		offset, _ := c.SatelliteOffset(ctx)
		if offset != 4 {
			t.Fatal(offset)
		}
		if err = c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "heartbeat", []byte(`{"offset":6}`), 6); err == nil {
			t.Fatal("heartbeat skipped a missing data offset")
		}
		if err = c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "heartbeat", []byte(`{"offset":5}`), 5); err != nil {
			t.Fatal("sequenced heartbeat not committed", err)
		}
		if offset, err := reopened.SatelliteOffset(ctx); err != nil || offset != 5 {
			t.Fatal("heartbeat checkpoint not durable", offset, err)
		}
		if err := c.pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_outbox WHERE source='SATELIT'`).Scan(&intents); err != nil || intents != 3 {
			t.Fatal("heartbeat generated a hazard event", intents, err)
		}
		_, _ = c.pool.Exec(ctx, `UPDATE source_leases SET lease_until=clock_timestamp()-interval '1 second' WHERE source='SATELIT'`)
		if err = c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", a, 6); !errors.Is(err, ErrLeaseLost) {
			t.Fatal("stale owner admitted", err)
		}
		reopened.Close()
	}
}

func TestM2InboxLargePayloadPreservedAndParkedIdentityIsolation(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	large := bytes.Repeat([]byte{255}, 2<<20)
	_, err := c.StageInbox(ctx, "large", "PVMBG", "bad", "volcanic-report", "hash", bytes.NewReader(large))
	if err != nil {
		t.Fatal(err)
	}
	done, err := c.ProcessInbox(ctx, "PVMBG", func(InboxMessage) (*model.HazardEvent, error) {
		t.Fatal("oversized payload reached decoder")
		return nil, nil
	})
	if !done || err != nil {
		t.Fatal(done, err)
	}
	var raw []byte
	for chunk := 0; ; chunk++ {
		details, err := c.InboxDetails(ctx, "large", chunk)
		if err != nil {
			t.Fatal(err)
		}
		part, _ := base64.StdEncoding.DecodeString(details["payload_base64"].(string))
		raw = append(raw, part...)
		if !details["has_more"].(bool) {
			break
		}
	}
	if !bytes.Equal(raw, large) {
		t.Fatal("quarantine truncated bytes")
	}
	for _, ref := range []string{"bad", "good"} {
		id := "msg-" + ref
		_, err = c.StageInbox(ctx, id, "PVMBG", ref, "volcanic-report", id, bytes.NewReader([]byte(`{}`)))
		if err != nil {
			t.Fatal(err)
		}
	}
	worked, err := c.ProcessInbox(ctx, "PVMBG", func(m InboxMessage) (*model.HazardEvent, error) {
		if m.Ref != "good" {
			t.Fatal("parked identity not blocked")
		}
		e := testHazard()
		e.Source = model.SourcePVMBG
		e.SourceRefID = m.Ref
		return &e, nil
	})
	if !worked || err != nil {
		t.Fatal(worked, err)
	}
	if n, err := c.ReplayInbox(ctx, "PVMBG"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}

func TestM2SchemaReplayPreservesPerIdentityOrder(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	for _, item := range []struct{ id, ref string }{{"old-a", "a"}, {"next-a", "a"}, {"other-b", "b"}} {
		if _, err := c.StageInbox(ctx, item.id, "PVMBG", item.ref, "volcanic-report", item.id, bytes.NewBufferString(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	compatible := false
	var seen []string
	convert := func(m InboxMessage) (*model.HazardEvent, error) {
		if m.ID == "old-a" && !compatible {
			return nil, fmt.Errorf("schema_incompatible: renamed field")
		}
		seen = append(seen, m.ID)
		e := testHazard()
		e.Source, e.SourceRefID = model.SourcePVMBG, m.Ref
		e.Attributes = map[string]any{"revision": m.ID}
		return &e, nil
	}
	for i := 0; i < 2; i++ {
		if worked, err := c.ProcessInbox(ctx, "PVMBG", convert); err != nil || !worked {
			t.Fatal(worked, err)
		}
	}
	if worked, err := c.ProcessInbox(ctx, "PVMBG", convert); err != nil || worked {
		t.Fatal("later same-identity revision bypassed parked predecessor", worked, err)
	}
	compatible = true
	if _, err := c.ReplayInbox(ctx, "PVMBG"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if worked, err := c.ProcessInbox(ctx, "PVMBG", convert); err != nil || !worked {
			t.Fatal(worked, err)
		}
	}
	if fmt.Sprint(seen) != "[other-b old-a next-a]" {
		t.Fatal("replay order", seen)
	}
	if _, err := c.ReplayInbox(ctx, "PVMBG"); err != nil {
		t.Fatal(err)
	}
	if worked, err := c.ProcessInbox(ctx, "PVMBG", convert); err != nil || worked {
		t.Fatal("completed replay ran again", worked, err)
	}
	var intents int
	if err := c.pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_outbox WHERE source='PVMBG'`).Scan(&intents); err != nil || intents != 3 {
		t.Fatal("missing or duplicate effects", intents, err)
	}
}

func TestM2AcceptedReportsSurviveReceiverRestartWithoutDuplicates(t *testing.T) {
	c, url := integrationCoordinator(t)
	ctx := t.Context()
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("field-%d", i)
		body := []byte(fmt.Sprintf(`{"client_id":"field","data":{"description":"report %d"}}`, i))
		for attempt := 0; attempt < 2; attempt++ {
			if _, err := c.StageInbox(ctx, id, "FIELD_REPORT", id, "field-report", id, bytes.NewReader(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	reopened, err := NewCoordinator(ctx, url, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for {
		worked, err := reopened.ProcessInbox(ctx, "FIELD_REPORT", func(InboxMessage) (*model.HazardEvent, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	var n int
	_ = c.pool.QueryRow(ctx, `SELECT count(*) FROM field_report_results`).Scan(&n)
	if n != 100 {
		t.Fatal("lost/duplicate acknowledged reports", n)
	}
	if _, err = c.StageInbox(ctx, "field-0", "FIELD_REPORT", "field-0", "field-report", "changed", bytes.NewReader([]byte(`{}`))); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal(err)
	}
}

func TestM2SatelliteFailedAdmissionRollsBackStateAndOffset(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	lease, _, _ := c.AcquireOrRenewLease(ctx, "SATELIT", "worker", time.Minute)
	if err := c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", satellitePayload("N20", -2, 20), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(ctx, `ALTER TABLE ingestion_outbox ADD CONSTRAINT inject_failure CHECK(source<>'SATELIT')`); err != nil {
		t.Fatal(err)
	}
	if err := c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", satellitePayload("N21", -2.001, 20), 1); err == nil {
		t.Fatal("failure injection did not reject transaction")
	}
	offset, _ := c.SatelliteOffset(ctx)
	if offset != 0 {
		t.Fatal("checkpoint advanced before durable result", offset)
	}
	var count int
	_ = c.pool.QueryRow(ctx, `SELECT count(*) FROM satellite_detections`).Scan(&count)
	if count != 1 {
		t.Fatal("partial state committed")
	}
	_, _ = c.pool.Exec(ctx, `ALTER TABLE ingestion_outbox DROP CONSTRAINT inject_failure`)
	if err := c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", satellitePayload("N21", -2.001, 20), 1); err != nil {
		t.Fatal(err)
	}
	_ = c.pool.QueryRow(ctx, `SELECT count(*) FROM satellite_events`).Scan(&count)
	if count != 1 {
		t.Fatal(count)
	}
}

func TestM2RetractionSurvivesStaleReplicaAndRepair(t *testing.T) {
	s, _, settings := integrationReplicas(t)
	ctx := t.Context()
	e := testHazard()
	e.Source = model.SourceSATELIT
	e.SourceRefID = "removed-fire"
	e.IngestedAt = time.Now().UTC()
	if err := s.UpsertAccepted(ctx, &e, 10); err != nil {
		t.Fatal(err)
	}
	pair := s.destinations(partitionFor(string(e.Source), e.SourceRefID))
	e.Attributes = map[string]any{"_retracted": true}
	if err := s.UpsertAcceptedTarget(ctx, &e, 11, "canonical:"+settings[pair[0]].Name); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetEventByID(ctx, e.HazardID)
	if err != nil || got.Event != nil {
		t.Fatal("stale peer resurrected removed fire", got, err)
	}
	s.reconcileReady(ctx)
	e.Attributes = map[string]any{}
	if err := s.UpsertAcceptedTarget(ctx, &e, 10, "canonical:"+settings[pair[1]].Name); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetEventByID(ctx, e.HazardID)
	if err != nil || got.Event != nil {
		t.Fatal("old replay resurrected tombstone", got, err)
	}
	checksum, err := s.AuditChecksum(ctx, "SATELIT")
	if err != nil || checksum.Count != 0 {
		t.Fatal(checksum, err)
	}
}

func TestM2AuditPaginatesPastRetractedFireHistory(t *testing.T) {
	s, _, _ := integrationReplicas(t)
	ctx := t.Context()
	for _, u := range s.units {
		_, err := u.pool.Exec(ctx, `INSERT INTO hazard_events
 (hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,
 occurred_at,ingested_at,attributes,partition_id,revision,ingestion_version)
 SELECT 'removed-'||i,'SATELIT','removed-'||i,'WILDFIRE','WASPADA','test',0,0,
 '2026-11-20'::timestamptz,'2026-11-20'::timestamptz,'{"_retracted":true}',0,1,i
 FROM generate_series(1,1100) i;
 INSERT INTO hazard_events
 (hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,
 occurred_at,ingested_at,attributes,partition_id,revision,ingestion_version)
 SELECT 'live-'||i,'SATELIT','live-'||i,'WILDFIRE','WASPADA','test',0,0,
 '2026-11-21'::timestamptz,'2026-11-21'::timestamptz,'{}',0,1,2000+i
 FROM generate_series(1,30) i`)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, limit := range []int{10, 1000} {
		cursor, count := "", 0
		for {
			page, err := s.ListEvents(ctx, EventFilter{Source: "SATELIT", Audit: true, Limit: limit, Cursor: cursor})
			if err != nil || page.Partial {
				t.Fatalf("healthy tombstone history made audit incomplete: %+v %v", page, err)
			}
			for _, e := range page.Events {
				if e.Attributes["_retracted"] == true {
					t.Fatal("tombstone escaped audit", e.HazardID)
				}
			}
			count += len(page.Events)
			if page.NextCursor == nil {
				break
			}
			cursor = *page.NextCursor
		}
		if count != 30 {
			t.Fatalf("audit lost current fires with limit %d: %d", limit, count)
		}
	}
}

func TestM2TransientInboxFailureRetainsReasonAndRetries(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	_, err := c.StageInbox(ctx, "retry-report", "FIELD_REPORT", "retry-report", "field-report", "hash",
		bytes.NewBufferString(`{"client_id":"field","data":{"description":"retained"}}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.pool.Exec(ctx, `ALTER TABLE field_report_results ADD CONSTRAINT reject_report CHECK(report_id<>'retry-report')`)
	if err != nil {
		t.Fatal(err)
	}
	convert := func(InboxMessage) (*model.HazardEvent, error) { return nil, nil }
	if _, err := c.ProcessInbox(ctx, "FIELD_REPORT", convert); err == nil {
		t.Fatal("downstream failure not injected")
	}
	var retryable bool
	err = c.pool.QueryRow(ctx, `SELECT status='retrying' AND reason LIKE 'downstream_retry:%' AND attempts=1
 AND next_attempt_at>clock_timestamp() AND completed_at IS NULL FROM durable_inbox WHERE message_id='retry-report'`).Scan(&retryable)
	if err != nil || !retryable {
		t.Fatal("transient failure not retained with backoff", retryable, err)
	}
	_, err = c.pool.Exec(ctx, `ALTER TABLE field_report_results DROP CONSTRAINT reject_report;
 UPDATE durable_inbox SET next_attempt_at=clock_timestamp() WHERE message_id='retry-report'`)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := c.ProcessInbox(ctx, "FIELD_REPORT", convert); !worked || err != nil {
		t.Fatal(worked, err)
	}
	var done bool
	err = c.pool.QueryRow(ctx, `SELECT status='done' AND reason='' AND attempts=2 FROM durable_inbox WHERE message_id='retry-report'`).Scan(&done)
	if err != nil || !done {
		t.Fatal(done, err)
	}
}

func TestM2SatelliteMergeSplitKeepsTransitiveMembers(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	lease, _, err := c.AcquireOrRenewLease(ctx, "SATELIT", "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for offset, lat := range []float64{-2, -2.001, -1.988, -1.987, -1.994} {
		if err := c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "detection", satellitePayload(fmt.Sprint("S", offset), lat, 10), int64(offset)); err != nil {
			t.Fatal(err)
		}
	}
	var fires, count int
	err = c.pool.QueryRow(ctx, `SELECT count(*),sum((attributes->>'detection_count')::int) FROM satellite_events`).Scan(&fires, &count)
	if err != nil || fires != 1 || count != 5 {
		t.Fatal("merge lost distant transitive members", fires, count, err)
	}
	if err := c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "update",
		[]byte(`{"key":"S4|2026-11-20|100|-1.994|113","changes":{"confidence":"l"}}`), 5); err != nil {
		t.Fatal(err)
	}
	err = c.pool.QueryRow(ctx, `SELECT count(*),sum((attributes->>'detection_count')::int) FROM satellite_events`).Scan(&fires, &count)
	if err != nil || fires != 2 || count != 4 {
		t.Fatal("split did not retain both components", fires, count, err)
	}
	if err := c.ProcessSatelliteEvent(ctx, "worker", lease.Token, "retract",
		[]byte(`{"key":"S1|2026-11-20|100|-2.001|113"}`), 6); err != nil {
		t.Fatal(err)
	}
	err = c.pool.QueryRow(ctx, `SELECT count(*),sum((attributes->>'detection_count')::int) FROM satellite_events`).Scan(&fires, &count)
	if err != nil || fires != 1 || count != 2 {
		t.Fatal("removal did not retract the singleton fire", fires, count, err)
	}
}

func TestM2SatelliteBatchRollsBackEveryFrameAndCheckpoint(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	lease, _, err := c.AcquireOrRenewLease(ctx, "SATELIT", "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	batch := []model.SatelliteMutation{
		{Type: "detection", Payload: satellitePayload("N20", -2, 20), Offset: 0, ReceivedAt: time.Now().Add(-time.Second)},
		{Type: "detection", Payload: satellitePayload("N21", -2.001, 20), Offset: 1, ReceivedAt: time.Now().Add(-time.Second)},
		{Type: "heartbeat", Payload: []byte(`{}`), Offset: 3},
	}
	if err := c.ProcessSatelliteBatch(ctx, "worker", lease.Token, batch); err == nil {
		t.Fatal("gap inside batch accepted")
	}
	var count int
	err = c.pool.QueryRow(ctx, `SELECT count(*) FROM satellite_detections`).Scan(&count)
	if err != nil || count != 0 {
		t.Fatal("part of failed batch committed", count, err)
	}
	if offset, _ := c.SatelliteOffset(ctx); offset != -1 {
		t.Fatal("failed batch advanced checkpoint", offset)
	}
	batch[2].Offset = 2
	for i := 0; i < 2; i++ {
		if err := c.ProcessSatelliteBatch(ctx, "worker", lease.Token, batch); err != nil {
			t.Fatal(err)
		}
	}
	err = c.pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_outbox`).Scan(&count)
	if err != nil || count != 1 {
		t.Fatal("batch replay duplicated effects", count, err)
	}
	var latencyIncludesQueue bool
	err = c.pool.QueryRow(ctx, `SELECT accepted_at<clock_timestamp()-interval '900 milliseconds' FROM ingestion_outbox`).Scan(&latencyIncludesQueue)
	if err != nil || !latencyIncludesQueue {
		t.Fatal("latency origin excludes batch queue wait", latencyIncludesQueue, err)
	}
}
