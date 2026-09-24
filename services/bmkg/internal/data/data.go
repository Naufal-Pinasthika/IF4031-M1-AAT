package data

import (
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/model"

)

// simple mockup data
var SeismicEvents = []model.SeismicEvent{
	{
		EventID:          "EQ-001",
		Magnitude:        6.2,
		DepthKM:          12.5,
		EpicenterLat:     -6.9147,
		EpicenterLon:     107.6098,
		RegionName:       "Jawa Barat",
		OccurredAt:       time.Date(2026, 9, 24, 9, 30, 0, 0, time.UTC),
		PotentialTsunami: false,
	},
	{
		EventID:          "EQ-002",
		Magnitude:        7.1,
		DepthKM:          20.0,
		EpicenterLat:     -8.2,
		EpicenterLon:     110.5,
		RegionName:       "Selatan Jawa",
		OccurredAt:       time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC),
		PotentialTsunami: true,
	},
}

var TsunamiWarnings = []model.TsunamiWarning{
	{
		WarningID:      "TW-001",
		RelatedEventID: "EQ-002",
		ThreatLevel:    model.ThreatSiaga,
		AffectedZones: []string{
			"Pantai Selatan Jawa Barat",
			"Pantai Selatan Jawa Tengah",
		},
		EstimatedArrival: time.Date(
			2026, 9, 24, 10, 45, 0, 0, time.UTC,
		),
	},
}