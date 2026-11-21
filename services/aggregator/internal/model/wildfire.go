package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SatelliteDetection struct {
	Satellite     string    `json:"satellite"`
	AcqDate       string    `json:"acq_date"`
	AcqTime       string    `json:"acq_time"`
	Latitude      float64   `json:"latitude"`
	Longitude     float64   `json:"longitude"`
	Confidence    string    `json:"confidence"`
	FRP           float64   `json:"frp"`
	DetectedAt    time.Time `json:"-"`
	LatitudeText  string    `json:"-"`
	LongitudeText string    `json:"-"`
}

func (d *SatelliteDetection) UnmarshalJSON(data []byte) error {
	var fields struct {
		Satellite  string          `json:"satellite"`
		AcqDate    string          `json:"acq_date"`
		AcqTime    json.RawMessage `json:"acq_time"`
		Latitude   float64         `json:"latitude"`
		Longitude  float64         `json:"longitude"`
		Confidence string          `json:"confidence"`
		FRP        float64         `json:"frp"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	acqTime, err := numericAcqTime(fields.AcqTime)
	if err != nil {
		return err
	}
	parsed, err := ParseDetectionTime(fields.AcqDate, acqTime)
	if err != nil {
		return err
	}
	*d = SatelliteDetection{
		Satellite:  fields.Satellite,
		AcqDate:    fields.AcqDate,
		AcqTime:    acqTime,
		Latitude:   fields.Latitude,
		Longitude:  fields.Longitude,
		Confidence: fields.Confidence,
		FRP:        fields.FRP,
		DetectedAt: parsed,
	}

	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(data, &rawFields); err != nil {
		return err
	}
	d.LatitudeText = rawCoordinateText(rawFields, "latitude")
	d.LongitudeText = rawCoordinateText(rawFields, "longitude")
	return nil
}

func numericAcqTime(raw json.RawMessage) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", fmt.Errorf("acq_time is required")
	}
	if raw[0] == '"' {
		return "", fmt.Errorf("acq_time must be a JSON number")
	}
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return "", fmt.Errorf("acq_time must be a JSON integer")
	}
	return strconv.FormatInt(value, 10), nil
}

func rawCoordinateText(fields map[string]json.RawMessage, name string) string {
	raw, ok := fields[name]
	if !ok {
		return ""
	}
	raw = bytes.TrimSpace(raw)
	if _, err := strconv.ParseFloat(string(raw), 64); err != nil {
		return ""
	}
	return string(raw)
}

const (
	MaxFireDistanceKM = 1.0
	MaxFireTime       = 360 * time.Minute
)

type FireCluster struct {
	Detections []SatelliteDetection
}

func IsLowConfidence(confidence string) bool {
	switch strings.ToLower(strings.TrimSpace(confidence)) {
	case "low", "l":
		return true
	default:
		return false
	}
}

func DetectionKey(detection SatelliteDetection) string {
	latitude := detection.LatitudeText
	if latitude == "" {
		latitude = strconv.FormatFloat(detection.Latitude, 'f', -1, 64)
	}
	longitude := detection.LongitudeText
	if longitude == "" {
		longitude = strconv.FormatFloat(detection.Longitude, 'f', -1, 64)
	}
	return strings.Join([]string{
		detection.Satellite,
		detection.AcqDate,
		detection.AcqTime,
		latitude,
		longitude,
	}, "|")
}

func DetectionClusterID(detection SatelliteDetection) string {
	sum := sha256.Sum256([]byte(DetectionKey(detection)))
	return "fire-" + hex.EncodeToString(sum[:])[:16]
}

func DetectionComponentClusterID(detections []SatelliteDetection) string {
	keys := detectionKeys(detections)
	sum := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return "fire-" + hex.EncodeToString(sum[:])[:16]
}

func detectionKeys(detections []SatelliteDetection) []string {
	keys := make([]string, 0, len(detections))
	for _, detection := range detections {
		keys = append(keys, DetectionKey(detection))
	}
	sort.Strings(keys)
	return keys
}

func DetectionsConnected(left, right SatelliteDetection) bool {
	if left.DetectedAt.Sub(right.DetectedAt) > MaxFireTime ||
		right.DetectedAt.Sub(left.DetectedAt) > MaxFireTime {
		return false
	}
	return HaversineKM(left.Latitude, left.Longitude, right.Latitude, right.Longitude) <= MaxFireDistanceKM
}

func HaversineKM(latitude1, longitude1, latitude2, longitude2 float64) float64 {
	const earthRadiusKM = 6371.0088
	latitudeDelta := (latitude2 - latitude1) * math.Pi / 180
	longitudeDelta := (longitude2 - longitude1) * math.Pi / 180
	latitude1Radians := latitude1 * math.Pi / 180
	latitude2Radians := latitude2 * math.Pi / 180
	a := math.Sin(latitudeDelta/2)*math.Sin(latitudeDelta/2) +
		math.Sin(longitudeDelta/2)*math.Sin(longitudeDelta/2)*math.Cos(latitude1Radians)*math.Cos(latitude2Radians)
	return 2 * earthRadiusKM * math.Asin(math.Sqrt(a))
}

func ClusterDetections(detections []SatelliteDetection) ([]FireCluster, []SatelliteDetection) {
	unique := make(map[string]SatelliteDetection, len(detections))
	for _, detection := range detections {
		if IsLowConfidence(detection.Confidence) {
			continue
		}
		key := DetectionKey(detection)
		if _, exists := unique[key]; !exists {
			unique[key] = detection
		}
	}
	all := make([]SatelliteDetection, 0, len(unique))
	for _, detection := range unique {
		all = append(all, detection)
	}
	sort.Slice(all, func(i, j int) bool {
		return DetectionKey(all[i]) < DetectionKey(all[j])
	})

	visited := make([]bool, len(all))
	var clusters []FireCluster
	var singles []SatelliteDetection
	for start := range all {
		if visited[start] {
			continue
		}
		queue := []int{start}
		visited[start] = true
		component := make([]SatelliteDetection, 0)
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			component = append(component, all[current])
			for candidate := range all {
				if visited[candidate] || !DetectionsConnected(all[current], all[candidate]) {
					continue
				}
				visited[candidate] = true
				queue = append(queue, candidate)
			}
		}
		sort.Slice(component, func(i, j int) bool {
			if component[i].DetectedAt.Equal(component[j].DetectedAt) {
				return DetectionKey(component[i]) < DetectionKey(component[j])
			}
			return component[i].DetectedAt.Before(component[j].DetectedAt)
		})
		if len(component) >= 2 {
			clusters = append(clusters, FireCluster{Detections: component})
		} else {
			singles = append(singles, component[0])
		}
	}
	sort.Slice(clusters, func(i, j int) bool {
		return DetectionKey(clusters[i].Detections[0]) < DetectionKey(clusters[j].Detections[0])
	})
	sort.Slice(singles, func(i, j int) bool {
		return DetectionKey(singles[i]) < DetectionKey(singles[j])
	})
	return clusters, singles
}

func ParseDetectionTime(acqDate, acqTime string) (time.Time, error) {
	acqTime = strings.TrimSpace(acqTime)
	date, err := time.Parse("2006-01-02", strings.TrimSpace(acqDate))
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid acq_date %q: %w", acqDate, err)
	}
	hour, minute := 0, 0
	if strings.Contains(acqTime, ":") {
		parsed, err := time.Parse("15:04", acqTime)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid acq_time %q: %w", acqTime, err)
		}
		hour, minute = parsed.Hour(), parsed.Minute()
	} else {
		value, err := strconv.Atoi(acqTime)
		if err != nil || value < 0 || value > 2359 {
			return time.Time{}, fmt.Errorf("invalid acq_time %q", acqTime)
		}
		hour, minute = value/100, value%100
		if minute >= 60 {
			return time.Time{}, fmt.Errorf("invalid acq_time %q", acqTime)
		}
	}
	return time.Date(date.Year(), date.Month(), date.Day(), hour, minute, 0, 0, time.UTC), nil
}
