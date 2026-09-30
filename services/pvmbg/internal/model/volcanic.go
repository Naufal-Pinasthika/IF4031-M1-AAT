package model

import "time"

type AlertLevel string

const (
	AlertNormal  AlertLevel = "Normal"
	AlertWaspada AlertLevel = "Waspada"
	AlertSiaga   AlertLevel = "Siaga"
	AlertAwas    AlertLevel = "Awas"
)

type VolcanicReport struct {
	ReportID         string     `json:"report_id"`
	VolcanoID        string     `json:"volcano_id"`
	AlertLevel       AlertLevel `json:"alert_level"`
	EruptionCount24H int64      `json:"eruption_count_24h"`
	AshColumnHeightM float64    `json:"ash_column_height_m"`
	ReportedAt       time.Time  `json:"reported_at"`
	// use omitempty to trigger confidence_level
	ConfidenceLevel *float64 `json:"confidence_level,omitempty"`
}
