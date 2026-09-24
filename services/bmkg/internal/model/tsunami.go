package model

import "time"

type ThreatLevel string

const (
	ThreatWaspada ThreatLevel = "Waspada"
	ThreatSiaga ThreatLevel = "Siaga"
	ThreatAwas ThreatLevel = "Awas"
)

type TsunamiWarning struct {
	WarningID string      	   `json:"warning_id"`
	RelatedEventID string      `json:"related_event_id"`
	ThreatLevel ThreatLevel    `json:"threat_level"`
	AffectedZones []string	   `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}