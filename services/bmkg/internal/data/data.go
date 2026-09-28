package data

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/model"
)

// mock mbkg data generator
var dataMu sync.RWMutex

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

func ListSeismicEvents(since time.Time) []model.SeismicEvent {
	dataMu.RLock()
	defer dataMu.RUnlock()

	result := make([]model.SeismicEvent, 0)

	for _, event := range SeismicEvents {
		if !event.OccurredAt.Before(since) {
			result = append(result, event)
		}
	}

	return result
}

func ListTsunamiWarnings(since time.Time) []model.TsunamiWarning {
	dataMu.RLock()
	defer dataMu.RUnlock()

	eventOccurredAt := make(map[string]time.Time, len(SeismicEvents))

	for _, event := range SeismicEvents {
		eventOccurredAt[event.EventID] = event.OccurredAt
	}

	result := make([]model.TsunamiWarning, 0)

	for _, warning := range TsunamiWarnings {
		occurredAt, exists := eventOccurredAt[warning.RelatedEventID]
		if !exists || occurredAt.Before(since) {
			continue
		}

		warning.AffectedZones = append([]string(nil), warning.AffectedZones...)
		result = append(result, warning)
	}

	return result
}

func StartGenerator(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			appendGeneratedEvent(now.UTC())

		case <-ctx.Done():
			return
		}
	}
}

func appendGeneratedEvent(now time.Time) {
	dataMu.Lock()
	defer dataMu.Unlock()

	index := len(SeismicEvents) + 1
	eventID := fmt.Sprintf("EQ-%03d", index)

	event := model.SeismicEvent{
		EventID:          eventID,
		Magnitude:        5.0 + float64(index%16)*0.15,
		DepthKM:          10.0 + float64(index%8),
		EpicenterLat:     -6.2,
		EpicenterLon:     106.8,
		RegionName:       "Demo Region",
		OccurredAt:       now,
		PotentialTsunami: index%4 == 0,
	}

	SeismicEvents = append(SeismicEvents, event)

	if !event.PotentialTsunami {
		return
	}

	threatLevel := model.ThreatWaspada

	if event.Magnitude >= 6.5 {
		threatLevel = model.ThreatSiaga
	}

	if event.Magnitude >= 7.0 {
		threatLevel = model.ThreatAwas
	}

	TsunamiWarnings = append(TsunamiWarnings, model.TsunamiWarning{
		WarningID:      fmt.Sprintf("TW-%03d", len(TsunamiWarnings)+1),
		RelatedEventID: event.EventID,
		ThreatLevel:    threatLevel,
		AffectedZones: []string{
			event.RegionName + " bagian selatan",
			event.RegionName + " wilayah pesisir",
		},
		EstimatedArrival: now.Add(45 * time.Minute),
	})
}