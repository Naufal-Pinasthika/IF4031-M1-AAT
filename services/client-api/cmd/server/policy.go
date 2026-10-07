package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	scopeSummary = "hazards:read:summary"
	scopeRaw     = "hazards:read:raw"
)

var (
	errInsufficientRawScope     = errors.New("raw hazard fields require hazards:read:raw")
	errInsufficientSummaryScope = errors.New("summary scope is required")
	summaryFields               = []string{"hazard_id", "source", "hazard_type", "severity", "area_name", "occurred_at", "ingested_at"}
	rawFields                   = []string{"source_ref_id", "latitude", "longitude", "attributes"}
	allFieldSet                 = fieldSet(append(append([]string{}, summaryFields...), rawFields...))
	rawFieldSet                 = fieldSet(rawFields)
)

type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

type EventsEnvelope struct {
	Events  []HazardEvent   `json:"events"`
	Sources json.RawMessage `json:"sources"`
}

type EventEnvelope struct {
	Event        HazardEvent     `json:"event"`
	SourceStatus json.RawMessage `json:"source_status"`
}

func selectFields(raw, scopes string) ([]string, error) {
	if !hasScope(scopes, scopeSummary) {
		return nil, errInsufficientSummaryScope
	}
	canReadRaw := hasScope(scopes, scopeRaw)
	if strings.TrimSpace(raw) == "" {
		if canReadRaw {
			return append(append([]string{}, summaryFields...), rawFields...), nil
		}
		return append([]string{}, summaryFields...), nil
	}

	fields := make([]string, 0)
	seen := make(map[string]bool)
	for _, value := range strings.Split(raw, ",") {
		field := strings.TrimSpace(value)
		if field == "" {
			return nil, errors.New("fields must be a comma-separated list of field names")
		}
		if !allFieldSet[field] {
			return nil, fmt.Errorf("unknown field %q", field)
		}
		if rawFieldSet[field] && !canReadRaw {
			return nil, errInsufficientRawScope
		}
		if !seen[field] {
			fields = append(fields, field)
			seen[field] = true
		}
	}
	return fields, nil
}

func projectEvent(event HazardEvent, fields []string) map[string]any {
	result := make(map[string]any, len(fields))
	for _, field := range fields {
		switch field {
		case "hazard_id":
			result[field] = event.HazardID
		case "source":
			result[field] = event.Source
		case "source_ref_id":
			result[field] = event.SourceRefID
		case "hazard_type":
			result[field] = event.HazardType
		case "severity":
			result[field] = event.Severity
		case "area_name":
			result[field] = event.AreaName
		case "latitude":
			result[field] = event.Latitude
		case "longitude":
			result[field] = event.Longitude
		case "occurred_at":
			result[field] = event.OccurredAt
		case "ingested_at":
			result[field] = event.IngestedAt
		case "attributes":
			result[field] = event.Attributes
		}
	}
	return result
}

func fieldSet(fields []string) map[string]bool {
	result := make(map[string]bool, len(fields))
	for _, field := range fields {
		result[field] = true
	}
	return result
}
