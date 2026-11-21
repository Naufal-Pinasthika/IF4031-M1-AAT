package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/jackc/pgx/v5"
)

func ensureSatelliteTx(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS satellite_events (
		 hazard_id TEXT NOT NULL, source TEXT NOT NULL, source_ref_id TEXT PRIMARY KEY,
		 hazard_type TEXT NOT NULL,severity TEXT NOT NULL,area_name TEXT NOT NULL,
		 latitude DOUBLE PRECISION NOT NULL,longitude DOUBLE PRECISION NOT NULL,
		 occurred_at TIMESTAMPTZ NOT NULL,ingested_at TIMESTAMPTZ NOT NULL,
		 attributes JSONB NOT NULL,partition_id INTEGER NOT NULL,revision BIGINT NOT NULL);
		CREATE TABLE IF NOT EXISTS satellite_feed_state (
			singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
			last_offset BIGINT NOT NULL DEFAULT -1,
			high_watermark TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		INSERT INTO satellite_feed_state(singleton,last_offset)
		VALUES(true,-1) ON CONFLICT DO NOTHING;

		CREATE TABLE IF NOT EXISTS satellite_clusters (
			cluster_id TEXT PRIMARY KEY,
			source_ref_id TEXT NOT NULL UNIQUE
		);

		CREATE TABLE IF NOT EXISTS satellite_detections (
			detection_key TEXT PRIMARY KEY,
			satellite TEXT NOT NULL,
			acq_date TEXT NOT NULL,
			acq_time TEXT NOT NULL,
			detected_at TIMESTAMPTZ NOT NULL,
			latitude DOUBLE PRECISION NOT NULL,
			longitude DOUBLE PRECISION NOT NULL,
			confidence TEXT NOT NULL,
			frp DOUBLE PRECISION NOT NULL DEFAULT 0,
			cluster_id TEXT,
			active BOOLEAN NOT NULL DEFAULT true,
			feed_offset BIGINT NOT NULL,
			observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS satellite_detection_time_idx
			ON satellite_detections(detected_at,latitude);
		CREATE INDEX IF NOT EXISTS satellite_detection_cluster_idx
			ON satellite_detections(cluster_id,active);

		CREATE TABLE IF NOT EXISTS satellite_late_detections (
			detection_key TEXT PRIMARY KEY,
			payload JSONB NOT NULL,
			reason TEXT NOT NULL,
			feed_offset BIGINT NOT NULL,
			observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);`)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		ALTER TABLE satellite_feed_state
		ADD COLUMN IF NOT EXISTS high_watermark TIMESTAMPTZ
		NOT NULL DEFAULT '1970-01-01 00:00:00+00';`)
	return err
}

func scanSatelliteDetection(rows pgx.Rows) (model.SatelliteDetection, string, string, error) {
	var detection model.SatelliteDetection
	var key string
	var clusterID *string
	err := rows.Scan(
		&key, &detection.Satellite, &detection.AcqDate, &detection.AcqTime,
		&detection.DetectedAt, &detection.Latitude, &detection.Longitude,
		&detection.Confidence, &detection.FRP, &clusterID)
	if err != nil {
		return model.SatelliteDetection{}, "", "", err
	}
	keyParts := strings.Split(key, "|")
	if len(keyParts) >= 5 {
		detection.LatitudeText = keyParts[len(keyParts)-2]
		detection.LongitudeText = keyParts[len(keyParts)-1]
	}
	cluster := ""
	if clusterID != nil {
		cluster = *clusterID
	}
	return detection, key, cluster, nil
}

func (s *Store) satelliteCandidates(ctx context.Context, tx pgx.Tx, anchors []model.SatelliteDetection) ([]model.SatelliteDetection, map[string]string, error) {
	byKey := make(map[string]model.SatelliteDetection)
	clusterByKey := make(map[string]string)

	load := func(anchor model.SatelliteDetection) error {
		windowStart := anchor.DetectedAt.Add(-model.MaxFireTime)
		windowEnd := anchor.DetectedAt.Add(model.MaxFireTime)
		rows, err := tx.Query(ctx, `
			SELECT detection_key, satellite, acq_date, acq_time, detected_at,
			       latitude, longitude, confidence, frp, cluster_id
			FROM satellite_detections
			WHERE active=true
			  AND detected_at BETWEEN $1 AND $2
			  AND ABS(latitude-$3) <= 0.02
			  AND ABS(longitude-$4) <= $5`,
			windowStart, windowEnd, anchor.Latitude, anchor.Longitude,
			1.0/(111.320*math.Cos(anchor.Latitude*math.Pi/180)))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			detection, key, clusterID, err := scanSatelliteDetection(rows)
			if err != nil {
				return err
			}
			if model.DetectionsConnected(anchor, detection) {
				byKey[key] = detection
				clusterByKey[key] = clusterID
			}
		}
		return rows.Err()
	}

	for _, anchor := range anchors {
		if err := load(anchor); err != nil {
			return nil, nil, err
		}
	}
	result := make([]model.SatelliteDetection, 0, len(byKey))
	for _, detection := range byKey {
		result = append(result, detection)
	}
	return result, clusterByKey, nil
}

func satelliteHazardEvent(clusterID string, detections []model.SatelliteDetection, keys []string) model.HazardEvent {
	latitude, longitude := 0.0, 0.0
	earliest := detections[0].DetectedAt
	totalFRP := 0.0
	satellites := make(map[string]bool)
	confidenceRank := map[string]int{"l": 1, "low": 1, "n": 2, "nominal": 2, "h": 3, "high": 3}
	maxConfidenceRank := 0
	maxConfidence := ""
	for _, detection := range detections {
		latitude += detection.Latitude
		longitude += detection.Longitude
		totalFRP += detection.FRP
		if detection.DetectedAt.Before(earliest) {
			earliest = detection.DetectedAt
		}
		satellites[detection.Satellite] = true
		normalized := strings.ToLower(detection.Confidence)
		if confidenceRank[normalized] > maxConfidenceRank {
			maxConfidenceRank = confidenceRank[normalized]
			maxConfidence = normalized
		}
	}
	count := len(detections)
	latitude /= float64(count)
	longitude /= float64(count)

	severity := model.SeverityWASPADA
	switch {
	case count >= 20 || totalFRP >= 100:
		severity = model.SeverityAWAS
	case count >= 6 || totalFRP >= 30:
		severity = model.SeveritySIAGA
	}
	satelliteList := make([]string, 0, len(satellites))
	for satellite := range satellites {
		satelliteList = append(satelliteList, satellite)
	}
	sort.Strings(satelliteList)
	sort.Strings(keys)
	totalFRP = math.Round(totalFRP*100) / 100

	return model.HazardEvent{
		HazardID:    eventID(string(model.SourceSATELIT), clusterID),
		Source:      model.SourceSATELIT,
		SourceRefID: clusterID,
		HazardType:  model.HazardTypeWILDFIRE,
		Severity:    severity,
		AreaName:    nearestProvinceName(latitude, longitude),
		Latitude:    latitude,
		Longitude:   longitude,
		OccuredAt:   earliest,
		Attributes: map[string]any{
			"detection_count": count,
			"satellites":      satelliteList,
			"max_confidence":  maxConfidence,
			"total_frp_mw":    totalFRP,
			"detection_keys":  keys,
		},
	}
}

func nearestProvinceName(latitude, longitude float64) string {
	provinces := []struct {
		name      string
		latitude  float64
		longitude float64
	}{
		{"Aceh", 4.695135, 96.749399},
		{"Sumatera Utara", 2.115354, 99.545097},
		{"Sumatera Barat", -0.739939, 100.800005},
		{"Riau", 0.506066, 101.5477},
		{"Jambi", -1.610122, 103.61312},
		{"Sumatera Selatan", -3.319437, 104.9145},
		{"Bengkulu", -3.792845, 102.260764},
		{"Lampung", -4.558584, 105.406807},
		{"Kepulauan Bangka Belitung", -2.741051, 106.440587},
		{"Kepulauan Riau", 3.945651, 108.142866},
		{"DKI Jakarta", -6.208763, 106.845599},
		{"Jawa Barat", -6.914744, 107.60981},
		{"Jawa Tengah", -7.150975, 110.140259},
		{"DI Yogyakarta", -7.7956, 110.3695},
		{"Jawa Timur", -7.536064, 112.238402},
		{"Banten", -6.405817, 106.064017},
		{"Bali", -8.409518, 115.188919},
		{"Nusa Tenggara Barat", -8.583333, 117.361648},
		{"Nusa Tenggara Timur", -8.657382, 121.07937},
		{"Kalimantan Barat", -0.278781, 111.475285},
		{"Kalimantan Tengah", -1.681487, 113.382354},
		{"Kalimantan Selatan", -3.092642, 115.283758},
		{"Kalimantan Timur", 0.538658, 116.419389},
		{"Kalimantan Utara", 3.073092, 116.041389},
		{"Sulawesi Utara", 1.493056, 125.0578},
		{"Sulawesi Tengah", -1.430025, 121.445618},
		{"Sulawesi Selatan", -3.668799, 119.974053},
		{"Sulawesi Tenggara", -4.14491, 122.174605},
		{"Gorontalo", 0.699937, 122.446723},
		{"Sulawesi Barat", -2.844137, 119.232078},
		{"Maluku", -3.238461, 130.145273},
		{"Maluku Utara", 0.790654, 127.7222},
		{"Papua Barat", -1.336115, 133.174716},
		{"Papua", -4.269928, 138.080352},
	}
	nearest := provinces[0].name
	nearestDistance := model.HaversineKM(latitude, longitude, provinces[0].latitude, provinces[0].longitude)
	for _, province := range provinces[1:] {
		distance := model.HaversineKM(latitude, longitude, province.latitude, province.longitude)
		if distance < nearestDistance {
			nearestDistance = distance
			nearest = province.name
		}
	}
	return nearest
}

func (s *Store) processSatelliteEventTx(ctx context.Context, tx pgx.Tx, eventType string, payload []byte, offset int64) ([]model.HazardEvent, []model.HazardEvent, error) {
	var lastOffset int64
	var highWatermark time.Time
	err := tx.QueryRow(ctx,
		`SELECT last_offset, high_watermark FROM satellite_feed_state WHERE singleton=true FOR UPDATE`,
	).Scan(&lastOffset, &highWatermark)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, err
	}
	if offset <= lastOffset {
		return nil, nil, nil
	}
	if offset != lastOffset+1 {
		return nil, nil, fmt.Errorf("satellite offset gap: committed %d, received %d", lastOffset, offset)
	}

	var target model.SatelliteDetection
	var oldKey string
	affectedClusterIDs := make(map[string]bool)
	reconciledAnchors := make([]model.SatelliteDetection, 0)
	clusterByKey := make(map[string]string)

	switch eventType {
	case "detection", "update":
		if eventType == "update" {
			target, oldKey, err = decodeSatelliteUpdate(payload)
		} else {
			target, oldKey, err = decodeSatelliteDetectionMutation(payload, eventType)
		}
		if err != nil {
			return nil, nil, err
		}
		if eventType == "detection" {
			key := model.DetectionKey(target)
			var existingKey string
			err = tx.QueryRow(ctx,
				`SELECT detection_key FROM satellite_detections WHERE detection_key=$1`,
				key).Scan(&existingKey)
			if err == nil {
				// Duplicate re-sends must not change active state or reset age.
				if _, err := tx.Exec(ctx, `
					INSERT INTO satellite_feed_state (singleton,last_offset,high_watermark,updated_at)
					VALUES (true,$1,$2,NOW())
					ON CONFLICT (singleton) DO UPDATE SET
						last_offset=GREATEST(satellite_feed_state.last_offset,EXCLUDED.last_offset),
						high_watermark=GREATEST(satellite_feed_state.high_watermark,EXCLUDED.high_watermark),
						updated_at=EXCLUDED.updated_at`,
					offset, highWatermark); err != nil {
					return nil, nil, err
				}
				return nil, nil, nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return nil, nil, err
			}
		}
		if eventType == "update" && oldKey != "" {
			var oldCluster *string
			err = tx.QueryRow(ctx,
				`SELECT cluster_id FROM satellite_detections WHERE detection_key=$1`,
				oldKey).Scan(&oldCluster)
			if err == nil && oldCluster != nil {
				affectedClusterIDs[*oldCluster] = true
			}
			var old model.SatelliteDetection
			var clusterIDIgnored *string
			err = tx.QueryRow(ctx, `
				SELECT detection_key,satellite,acq_date,acq_time,detected_at,
				       latitude,longitude,confidence,frp,cluster_id
				FROM satellite_detections
				WHERE detection_key=$1`,
				oldKey).Scan(
				&oldKey, &old.Satellite, &old.AcqDate, &old.AcqTime, &old.DetectedAt,
				&old.Latitude, &old.Longitude, &old.Confidence, &old.FRP, &clusterIDIgnored)
			if err == nil {
				if target.Satellite != "" {
					// Coordinate lexemes are retained separately from their numeric values.
					old.Satellite = target.Satellite
				}
				parts := strings.Split(oldKey, "|")
				if len(parts) == 5 {
					old.LatitudeText = parts[3]
					old.LongitudeText = parts[4]
				}
				if target.AcqDate != "" {
					old.AcqDate = target.AcqDate
				}
				if target.AcqTime != "" {
					old.AcqTime = target.AcqTime
				}
				if satellitePatchHas(payload, "latitude") {
					old.Latitude = target.Latitude
					old.LatitudeText = target.LatitudeText
				}
				if satellitePatchHas(payload, "longitude") {
					old.Longitude = target.Longitude
					old.LongitudeText = target.LongitudeText
				}
				if target.Confidence != "" {
					old.Confidence = target.Confidence
				}
				if satellitePatchHas(payload, "frp") {
					old.FRP = target.FRP
				}
				old.DetectedAt, err = model.ParseDetectionTime(old.AcqDate, old.AcqTime)
				if err != nil {
					return nil, nil, err
				}
				target = old
			}
			_, err = tx.Exec(ctx,
				`DELETE FROM satellite_detections WHERE detection_key=$1`,
				oldKey)
			if err != nil {
				return nil, nil, err
			}
		}

		key := model.DetectionKey(target)
		late := eventType == "detection" &&
			!highWatermark.IsZero() &&
			highWatermark.Sub(target.DetectedAt) > model.MaxFireTime
		if target.DetectedAt.After(highWatermark) {
			highWatermark = target.DetectedAt
		}

		active := !model.IsLowConfidence(target.Confidence) && !late
		_, err = tx.Exec(ctx, `
			INSERT INTO satellite_detections (
				detection_key, satellite, acq_date, acq_time, detected_at,
				latitude, longitude, confidence, frp, cluster_id, active, feed_offset
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,$10,$11)
			ON CONFLICT (detection_key) DO UPDATE SET
				satellite=EXCLUDED.satellite, acq_date=EXCLUDED.acq_date,
				acq_time=EXCLUDED.acq_time, detected_at=EXCLUDED.detected_at,
				latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
				confidence=EXCLUDED.confidence, frp=EXCLUDED.frp,
				active=EXCLUDED.active, feed_offset=EXCLUDED.feed_offset`,
			key, target.Satellite, target.AcqDate, target.AcqTime,
			target.DetectedAt, target.Latitude, target.Longitude,
			target.Confidence, target.FRP, active, offset)
		if err != nil {
			return nil, nil, err
		}
		if late {
			_, err = tx.Exec(ctx, `
				INSERT INTO satellite_late_detections (
					detection_key,payload,reason,feed_offset,observed_at
				)
				VALUES ($1,$2::jsonb,'late_by_satellite_detection_time',$3,NOW())
				ON CONFLICT (detection_key) DO UPDATE SET
					payload=EXCLUDED.payload,reason=EXCLUDED.reason,
					feed_offset=EXCLUDED.feed_offset,observed_at=NOW()`,
				key, payload, offset)
			if err != nil {
				return nil, nil, err
			}
		}
		if active {
			reconciledAnchors = append(reconciledAnchors, target)
		}

	case "retract":
		key, err := decodeSatelliteRetractKey(payload)
		if err != nil {
			_, err = tx.Exec(ctx, `
				INSERT INTO satellite_late_detections (
					detection_key,payload,reason,feed_offset,observed_at
				)
				VALUES ($1,$2::jsonb,'unidentified_retract',$3,NOW())
				ON CONFLICT (detection_key) DO UPDATE SET
					payload=EXCLUDED.payload,reason=EXCLUDED.reason,
					feed_offset=EXCLUDED.feed_offset,observed_at=NOW()`,
				fmt.Sprintf("unidentified-retract:%d", offset), payload, offset)
			if err != nil {
				return nil, nil, err
			}
			break
		}
		var oldCluster *string
		err = tx.QueryRow(ctx,
			`SELECT cluster_id FROM satellite_detections WHERE detection_key=$1`,
			key).Scan(&oldCluster)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, err
		}
		if oldCluster != nil {
			affectedClusterIDs[*oldCluster] = true
		}
		_, err = tx.Exec(ctx,
			`UPDATE satellite_detections SET active=false, cluster_id=NULL WHERE detection_key=$1`,
			key)
		if err != nil {
			return nil, nil, err
		}

	case "heartbeat":
		// No cluster mutation.
	default:
		return nil, nil, fmt.Errorf("unsupported satellite event type %q", eventType)
	}

	if eventType == "detection" || eventType == "update" || eventType == "retract" {
		reconciledAnchors, clusterByKey, err = s.loadAffectedSatelliteComponents(
			ctx, tx, affectedClusterIDs, reconciledAnchors)
		if err != nil {
			return nil, nil, err
		}
	}

	var events []model.HazardEvent
	var deleted []model.HazardEvent
	if eventType != "heartbeat" && eventType != "" {
		events, deleted, err = s.reconcileSatelliteComponents(
			ctx, tx, affectedClusterIDs, reconciledAnchors, clusterByKey)
		if err != nil {
			return nil, nil, err
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO satellite_feed_state (singleton,last_offset,high_watermark,updated_at)
		VALUES (true,$1,$2,NOW())
		ON CONFLICT (singleton) DO UPDATE SET
			last_offset=GREATEST(satellite_feed_state.last_offset,EXCLUDED.last_offset),
			high_watermark=GREATEST(satellite_feed_state.high_watermark,EXCLUDED.high_watermark),
			updated_at=EXCLUDED.updated_at`,
		offset, highWatermark)
	if err != nil {
		return nil, nil, err
	}
	return events, deleted, nil
}

func satellitePatchHas(payload []byte, key string) bool {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(payload, &fields)
	for _, name := range []string{"changes", "detection", "new", "current", "data"} {
		if raw, ok := fields[name]; ok {
			_ = json.Unmarshal(raw, &fields)
			break
		}
	}
	_, ok := fields[key]
	return ok
}

func decodeSatelliteDetectionMutation(payload []byte, eventType string) (model.SatelliteDetection, string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite %s payload: %w", eventType, err)
	}

	detectionPayload := payload
	for _, name := range []string{"detection", "new", "current", "data"} {
		if raw, ok := fields[name]; ok && len(raw) > 0 {
			detectionPayload = raw
			break
		}
	}

	var detection model.SatelliteDetection
	if err := json.Unmarshal(detectionPayload, &detection); err != nil {
		return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite detection: %w", err)
	}

	oldKey := ""
	for _, name := range []string{"old_detection_key", "previous_detection_key", "detection_key"} {
		if raw, ok := fields[name]; ok {
			if err := json.Unmarshal(raw, &oldKey); err == nil && oldKey != "" {
				break
			}
		}
	}
	if oldKey == "" {
		for _, name := range []string{"old", "previous"} {
			if raw, ok := fields[name]; ok {
				var old model.SatelliteDetection
				if err := json.Unmarshal(raw, &old); err == nil && old.Satellite != "" {
					oldKey = model.DetectionKey(old)
					break
				}
			}
		}
	}
	return detection, oldKey, nil
}

func decodeSatelliteUpdate(payload []byte) (model.SatelliteDetection, string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite update payload: %w", err)
	}

	oldKey := ""
	for _, name := range []string{"detection_key", "old_detection_key", "previous_detection_key", "key", "id"} {
		if raw, ok := fields[name]; ok {
			var key string
			if err := json.Unmarshal(raw, &key); err == nil && key != "" {
				oldKey = key
				break
			}
		}
	}

	for _, raw := range fields {
		var key string
		if err := json.Unmarshal(raw, &key); err == nil &&
			strings.Count(key, "|") >= 4 {
			if oldKey == "" {
				oldKey = key
			}
		}
	}

	if oldKey == "" {
		for _, name := range []string{"old", "previous", "data"} {
			if raw, ok := fields[name]; ok {
				var old model.SatelliteDetection
				if err := json.Unmarshal(raw, &old); err == nil && old.Satellite != "" {
					oldKey = model.DetectionKey(old)
					break
				}
			}
		}
	}
	if oldKey == "" {
		return model.SatelliteDetection{}, "", fmt.Errorf("update payload has no detection identity")
	}

	updatePayload := payload
	for _, name := range []string{"changes", "detection", "new", "current", "data"} {
		if raw, ok := fields[name]; ok {
			updatePayload = raw
			break
		}
	}

	var patch map[string]json.RawMessage
	if err := json.Unmarshal(updatePayload, &patch); err != nil {
		return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite update patch: %w", err)
	}
	if _, isNested := patch["detection"]; isNested {
		if raw, ok := patch["detection"]; ok {
			updatePayload = raw
		}
	}

	var patchFields map[string]json.RawMessage
	if err := json.Unmarshal(updatePayload, &patchFields); err != nil {
		return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite update patch: %w", err)
	}
	var target model.SatelliteDetection
	if raw, ok := patchFields["confidence"]; ok {
		if err := json.Unmarshal(raw, &target.Confidence); err != nil {
			return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite update confidence: %w", err)
		}
	}
	if raw, ok := patchFields["frp"]; ok {
		if err := json.Unmarshal(raw, &target.FRP); err != nil {
			return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite update frp: %w", err)
		}
	}
	if raw, ok := patchFields["latitude"]; ok {
		target.LatitudeText = string(raw)
		if err := json.Unmarshal(raw, &target.Latitude); err != nil {
			return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite update latitude: %w", err)
		}
	}
	if raw, ok := patchFields["longitude"]; ok {
		target.LongitudeText = string(raw)
		if err := json.Unmarshal(raw, &target.Longitude); err != nil {
			return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite update longitude: %w", err)
		}
	}
	if raw, ok := patchFields["satellite"]; ok {
		if err := json.Unmarshal(raw, &target.Satellite); err != nil {
			return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite update satellite: %w", err)
		}
	}
	if raw, ok := patchFields["acq_date"]; ok {
		if err := json.Unmarshal(raw, &target.AcqDate); err != nil {
			return model.SatelliteDetection{}, "", fmt.Errorf("decode satellite update acq_date: %w", err)
		}
	}
	if raw, ok := patchFields["acq_time"]; ok {
		raw = bytes.TrimSpace(raw)
		if raw[0] == '"' {
			return model.SatelliteDetection{}, "", fmt.Errorf("acq_time must be a JSON integer")
		}
		acqTimeValue, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return model.SatelliteDetection{}, "", fmt.Errorf("acq_time must be a JSON integer")
		}
		acqTime := strconv.FormatInt(acqTimeValue, 10)
		target.AcqTime = acqTime
	}
	return target, oldKey, nil
}

func decodeSatelliteRetractKey(payload []byte) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return "", fmt.Errorf("decode satellite retract payload: %w", err)
	}

	for _, name := range []string{"detection_key", "old_detection_key", "previous_detection_key", "key", "id", "detection_id", "hotspot_id"} {
		if raw, ok := fields[name]; ok {
			var key string
			if err := json.Unmarshal(raw, &key); err == nil && key != "" {
				return key, nil
			}
		}
	}

	for _, raw := range fields {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil &&
			strings.Count(value, "|") >= 4 {
			return value, nil
		}
	}

	canonical := true
	for _, name := range []string{"satellite", "acq_date", "acq_time", "latitude", "longitude"} {
		if _, ok := fields[name]; !ok {
			canonical = false
			break
		}
	}
	if canonical {
		var detection model.SatelliteDetection
		if err := json.Unmarshal(payload, &detection); err == nil {
			return model.DetectionKey(detection), nil
		}
	}

	for _, name := range []string{"detection", "old", "previous", "data"} {
		if raw, ok := fields[name]; ok {
			var key string
			if err := json.Unmarshal(raw, &key); err == nil &&
				strings.Count(key, "|") >= 4 {
				return key, nil
			}
			var detection model.SatelliteDetection
			if err := json.Unmarshal(raw, &detection); err == nil {
				return model.DetectionKey(detection), nil
			}
		}
	}

	return "", fmt.Errorf("retract payload has no detection_key or detection identity")
}

func (s *Store) loadAffectedSatelliteComponents(
	ctx context.Context,
	tx pgx.Tx,
	affectedClusterIDs map[string]bool,
	anchors []model.SatelliteDetection,
) ([]model.SatelliteDetection, map[string]string, error) {
	byKey := make(map[string]model.SatelliteDetection)
	clusterByKey := make(map[string]string)

	loadRows := func(rows pgx.Rows) error {
		defer rows.Close()
		for rows.Next() {
			detection, key, clusterID, err := scanSatelliteDetection(rows)
			if err != nil {
				return err
			}
			byKey[key] = detection
			if clusterID != "" {
				clusterByKey[key] = clusterID
			}
		}
		return rows.Err()
	}

	for clusterID := range affectedClusterIDs {
		rows, err := tx.Query(ctx, `
			SELECT detection_key, satellite, acq_date, acq_time, detected_at,
			       latitude, longitude, confidence, frp, cluster_id
			FROM satellite_detections
			WHERE active=true AND cluster_id=$1`,
			clusterID)
		if err != nil {
			return nil, nil, err
		}
		if err := loadRows(rows); err != nil {
			return nil, nil, err
		}
	}
	// Existing components were already transitively closed. Only new/changed
	// detections can connect them to another component; a removal cannot add an
	// edge. Probing every unchanged member repeats the same historical searches.

	candidates, candidateClusters, err := s.satelliteCandidates(ctx, tx, anchors)
	if err != nil {
		return nil, nil, err
	}

	for _, detection := range candidates {
		key := model.DetectionKey(detection)
		byKey[key] = detection
		if clusterID := candidateClusters[key]; clusterID != "" {
			clusterByKey[key] = clusterID
			affectedClusterIDs[clusterID] = true
		}
	}

	// A candidate only proves that this detection touches an existing fire.
	// Load every active member of that fire before reclustering, including
	// members that are not direct neighbors of the incoming detection.
	for clusterID := range affectedClusterIDs {
		rows, err := tx.Query(ctx, `
			SELECT detection_key, satellite, acq_date, acq_time, detected_at,
			       latitude, longitude, confidence, frp, cluster_id
			FROM satellite_detections
			WHERE active=true AND cluster_id=$1`,
			clusterID)
		if err != nil {
			return nil, nil, err
		}
		if err := loadRows(rows); err != nil {
			return nil, nil, err
		}
	}
	result := make([]model.SatelliteDetection, 0, len(byKey))
	for _, detection := range byKey {
		result = append(result, detection)
	}
	return result, clusterByKey, nil
}

func (s *Store) reconcileSatelliteComponents(
	ctx context.Context,
	tx pgx.Tx,
	affectedClusterIDs map[string]bool,
	detections []model.SatelliteDetection,
	clusterByKey map[string]string,
) ([]model.HazardEvent, []model.HazardEvent, error) {
	components, _ := model.ClusterDetections(detections)

	affectedKeys := make([]string, 0, len(detections))
	for _, detection := range detections {
		affectedKeys = append(affectedKeys, model.DetectionKey(detection))
	}
	assignedKeys := make([]string, 0, len(detections))
	for _, component := range components {
		for _, detection := range component.Detections {
			assignedKeys = append(assignedKeys, model.DetectionKey(detection))
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE satellite_detections SET cluster_id=NULL
 WHERE detection_key=ANY($1::text[]) AND NOT(detection_key=ANY($2::text[])) AND cluster_id IS NOT NULL`,
		affectedKeys, assignedKeys); err != nil {
		return nil, nil, err
	}

	chosenIDs := make(map[string]bool)
	usedExistingIDs := make(map[string]bool)
	events := make([]model.HazardEvent, 0, len(components))
	deleted := make([]model.HazardEvent, 0)
	for _, component := range components {
		clusterID := ""
		type clusterCandidate struct {
			id         string
			detectedAt time.Time
			key        string
		}
		existingIDs := make([]clusterCandidate, 0)
		for _, detection := range component.Detections {
			if existing := clusterByKey[model.DetectionKey(detection)]; existing != "" {
				found := false
				for _, candidate := range existingIDs {
					if candidate.id == existing {
						found = true
						break
					}
				}
				if !found {
					existingIDs = append(existingIDs, clusterCandidate{
						id:         existing,
						detectedAt: detection.DetectedAt,
						key:        model.DetectionKey(detection),
					})
				}
			}
		}
		sort.Slice(existingIDs, func(i, j int) bool {
			if existingIDs[i].detectedAt.Equal(existingIDs[j].detectedAt) {
				return existingIDs[i].key < existingIDs[j].key
			}
			return existingIDs[i].detectedAt.Before(existingIDs[j].detectedAt)
		})
		for _, existing := range existingIDs {
			if !usedExistingIDs[existing.id] {
				clusterID = existing.id
				usedExistingIDs[existing.id] = true
				break
			}
		}
		if clusterID == "" {
			clusterID = model.DetectionComponentClusterID(component.Detections)
		}
		chosenIDs[clusterID] = true

		keys := make([]string, 0, len(component.Detections))
		for _, detection := range component.Detections {
			key := model.DetectionKey(detection)
			keys = append(keys, key)
		}
		if _, err := tx.Exec(ctx, `UPDATE satellite_detections SET cluster_id=$1
 WHERE detection_key=ANY($2::text[]) AND cluster_id IS DISTINCT FROM $1`, clusterID, keys); err != nil {
			return nil, nil, err
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO satellite_clusters (cluster_id,source_ref_id)
			VALUES ($1,$1)
			ON CONFLICT (cluster_id) DO NOTHING`,
			clusterID)
		if err != nil {
			return nil, nil, err
		}

		event := satelliteHazardEvent(clusterID, component.Detections, keys)
		event.IngestedAt = time.Now().UTC()
		attributes, err := json.Marshal(event.Attributes)
		if err != nil {
			return nil, nil, err
		}
		partition := partitionFor(string(event.Source), event.SourceRefID)
		revision := int64(1) // mutations are serialized by the database lease row
		_, err = tx.Exec(ctx, `
			INSERT INTO satellite_events (
				hazard_id,source,source_ref_id,hazard_type,severity,
				area_name,latitude,longitude,occurred_at,ingested_at,
				attributes,partition_id,revision
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13)
			ON CONFLICT (source_ref_id) DO UPDATE SET
				hazard_type=EXCLUDED.hazard_type,severity=EXCLUDED.severity,
				area_name=EXCLUDED.area_name,latitude=EXCLUDED.latitude,
				longitude=EXCLUDED.longitude,occurred_at=EXCLUDED.occurred_at,
				ingested_at=EXCLUDED.ingested_at,attributes=EXCLUDED.attributes,
				partition_id=EXCLUDED.partition_id,revision=satellite_events.revision+1`,
			event.HazardID, event.Source, event.SourceRefID, event.HazardType,
			event.Severity, event.AreaName, event.Latitude, event.Longitude,
			event.OccuredAt, event.IngestedAt, attributes, partition, revision)
		if err != nil {
			return nil, nil, err
		}
		events = append(events, event)
	}

	for oldID := range affectedClusterIDs {
		if chosenIDs[oldID] {
			continue
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM satellite_events WHERE source='SATELIT' AND source_ref_id=$1`,
			oldID); err != nil {
			return nil, nil, err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM satellite_clusters WHERE cluster_id=$1`,
			oldID); err != nil {
			return nil, nil, err
		}
		deleted = append(deleted, model.HazardEvent{
			HazardID:    eventID(string(model.SourceSATELIT), oldID),
			Source:      model.SourceSATELIT,
			SourceRefID: oldID,
			HazardType:  model.HazardTypeWILDFIRE,
		})
	}
	return events, deleted, nil
}
