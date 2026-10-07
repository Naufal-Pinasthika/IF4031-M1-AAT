package event

import (
	"encoding/json"
	"fmt"
	"time"
)

const UpsertedType = "hazard.event.upserted"

type Envelope struct {
	EventType     string      `json:"event_type"`
	SchemaVersion int         `json:"schema_version"`
	CorrelationID string      `json:"correlation_id"`
	PublishedAt   time.Time   `json:"published_at"`
	Data          HazardEvent `json:"data"`
}

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

func Decode(payload []byte) (Envelope, error) {
	var envelope Envelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode event envelope: %w", err)
	}
	if envelope.EventType != UpsertedType {
		return Envelope{}, fmt.Errorf("unsupported event type %q", envelope.EventType)
	}
	if envelope.SchemaVersion != 1 {
		return Envelope{}, fmt.Errorf("unsupported schema version %d", envelope.SchemaVersion)
	}
	if envelope.Data.HazardID == "" || envelope.Data.SourceRefID == "" {
		return Envelope{}, fmt.Errorf("hazard_id and source_ref_id are required")
	}
	return envelope, nil
}
