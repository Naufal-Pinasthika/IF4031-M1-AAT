package model

import "time"

type HazardSource string

const (
	SourceBMKG  HazardSource = "BMKG"
	SourcePVMBG HazardSource = "PVMBG"
)

type HazardType string

const (
	HazardTypeSEISMIC  HazardType = "SEISMIC"
	HazardTypeVOLCANIC HazardType = "VOLCANIC"
)

type SeverityLevel string

const (
	SeverityNORMAL  SeverityLevel = "NORMAL"
	SeverityWASPADA SeverityLevel = "WASPADA"
	SeveritySIAGA   SeverityLevel = "SIAGA"
	SeverityAWAS    SeverityLevel = "AWAS"
)

type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      HazardSource   `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  HazardType     `json:"hazard_type"`
	Severity    SeverityLevel  `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccuredAt   time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}
