package store

import (
	"context"
	"sort"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

// p1
type DailyCounts struct {
	Date   string         `json:"date"`
	Counts map[string]int `json:"counts"`
}

// p1
type Analytics struct {
	Total      int            `json:"total"`
	ByHazard   map[string]int `json:"by_hazard"`
	Daily      []DailyCounts  `json:"daily"`
	BySeverity map[string]int `json:"by_severity"`
	ByArea     map[string]int `json:"by_area"`
	ReadStatus
}

// p1
func hazardCounts() map[string]int {
	return map[string]int{"SEISMIC": 0, "TSUNAMI": 0, "VOLCANIC": 0, "WILDFIRE": 0, "COASTAL": 0}
}

// p1 bucket on UTC occurrence dates, using the same canonical scan as audit
func (s *Store) Analytics(ctx context.Context, from, to *time.Time) (Analytics, error) {
	result := Analytics{ByHazard: hazardCounts(), Daily: []DailyCounts{}, BySeverity: map[string]int{"NORMAL": 0, "WASPADA": 0, "SIAGA": 0, "AWAS": 0}, ByArea: map[string]int{}}
	days := map[string]map[string]int{}
	status, err := s.forEach(ctx, EventFilter{From: from, To: to}, func(e model.HazardEvent) {
		result.Total++
		result.ByHazard[string(e.HazardType)]++
		result.BySeverity[string(e.Severity)]++
		result.ByArea[e.AreaName]++
		day := e.OccuredAt.UTC().Format("2006-01-02")
		if days[day] == nil {
			days[day] = hazardCounts()
		}
		days[day][string(e.HazardType)]++
	})
	for day, counts := range days {
		result.Daily = append(result.Daily, DailyCounts{day, counts})
	}
	sort.Slice(result.Daily, func(i, j int) bool { return result.Daily[i].Date < result.Daily[j].Date })
	result.ReadStatus = status
	return result, err
}
