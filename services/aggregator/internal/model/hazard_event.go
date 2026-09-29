package model

import "time"

type SourceEnum string

const (
	BMKG  SourceEnum = "BMKG"
	PVMBG SourceEnum = "PVMBG"
)

type HazardEnum string

const (
	SEISMIC  HazardEnum = "SEISMIC"
	VOLCANIC HazardEnum = "VOLCANIC"
)

type SeverityLevel string

const (
	NORMAL  SeverityLevel = "NORMAL"
	WASPADA SeverityLevel = "WASPADA"
	SIAGA   SeverityLevel = "SIAGA"
	AWAS    SeverityLevel = "AWAS"
)

type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      SourceEnum     `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  HazardEnum     `json:"hazard_type"`
	Severity    SeverityLevel  `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccuredAt   time.Time      `json:"occured_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}
