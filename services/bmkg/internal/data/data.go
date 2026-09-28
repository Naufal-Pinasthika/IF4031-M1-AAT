package data

import (
	"fmt"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/model"
)

// mock mbkg data generator
var SeismicEvents = seedSeismicEvents()

var TsunamiWarnings = seedTsunamiWarnings()

func seedSeismicEvents() []model.SeismicEvent {
	events := make([]model.SeismicEvent, 0, 20)

	baseTime := time.Date(
		2026,
		9,
		1,
		0,
		0,
		0,
		0,
		time.UTC,
	)

	regions := []string{
		"Jawa Barat",
		"Jawa Tengah",
		"Jawa Timur",
		"Sumatera Barat",
		"Sumatera Utara",
		"Selatan Jawa",
		"Bali",
		"Nusa Tenggara Barat",
		"Maluku",
		"Papua Barat",
	}

	for i := 0; i < 20; i++ {
		magnitude := 5.0 + float64(i%16)*0.15

		events = append(events, model.SeismicEvent{
			EventID:          fmt.Sprintf("EQ-%03d", i+1),
			Magnitude:        magnitude,
			DepthKM:          8.0 + float64(i%10)*2.5,
			EpicenterLat:     -9.0 + float64(i)*0.25,
			EpicenterLon:     105.0 + float64(i)*0.4,
			RegionName:       regions[i%len(regions)],
			OccurredAt:       baseTime.Add(time.Duration(i) * 12 * time.Hour),
			PotentialTsunami: i%4 == 0,
		})
	}

	return events
}

func seedTsunamiWarnings() []model.TsunamiWarning {
	warnings := make([]model.TsunamiWarning, 0)

	for _, event := range SeismicEvents {
		if !event.PotentialTsunami {
			continue
		}

		threatLevel := model.ThreatWaspada

		if event.Magnitude >= 6.5 {
			threatLevel = model.ThreatSiaga
		}

		if event.Magnitude >= 7.0 {
			threatLevel = model.ThreatAwas
		}

		warnings = append(warnings, model.TsunamiWarning{
			WarningID:      fmt.Sprintf("TW-%03d", len(warnings)+1),
			RelatedEventID: event.EventID,
			ThreatLevel:    threatLevel,
			AffectedZones: []string{
				event.RegionName + " bagian selatan",
				event.RegionName + " wilayah pesisir",
			},
			EstimatedArrival: event.OccurredAt.Add(45 * time.Minute),
		})
	}

	return warnings
}