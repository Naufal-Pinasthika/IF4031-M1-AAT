package store

import (
	"context"
	"sort"
	"time"
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

func newAnalytics() Analytics {
	return Analytics{ByHazard: hazardCounts(), Daily: []DailyCounts{}, BySeverity: map[string]int{"NORMAL": 0, "WASPADA": 0, "SIAGA": 0, "AWAS": 0}, ByArea: map[string]int{}, ReadStatus: ReadStatus{UnavailableUnits: []string{}}}
}

// Stream immutable identities in bounded batches, reconcile both replica
// versions, then aggregate. No full-event pagination, history cache or stale TTL.
func (s *Store) Analytics(ctx context.Context, from, to *time.Time) (Analytics, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type response struct {
		value Analytics
		err   error
	}
	partitions := len(s.units)
	responses := make(chan response, partitions)
	for partition := range partitions {
		go func() {
			value, err := s.analyticalPartition(ctx, partition, from, to)
			responses <- response{value, err}
		}()
	}
	result := newAnalytics()
	days := map[string]map[string]int{}
	var firstError error
	for range partitions {
		r := <-responses
		if r.err != nil {
			if firstError == nil {
				firstError = r.err
				cancel()
			}
			continue
		}
		result.Total += r.value.Total
		result.ReadStatus = mergeStatus(result.ReadStatus, r.value.ReadStatus)
		for key, count := range r.value.ByHazard {
			result.ByHazard[key] += count
		}
		for key, count := range r.value.BySeverity {
			result.BySeverity[key] += count
		}
		for key, count := range r.value.ByArea {
			result.ByArea[key] += count
		}
		for _, day := range r.value.Daily {
			if days[day.Date] == nil {
				days[day.Date] = hazardCounts()
			}
			for key, count := range day.Counts {
				days[day.Date][key] += count
			}
		}
	}
	if firstError != nil {
		return result, firstError
	}
	for day, counts := range days {
		result.Daily = append(result.Daily, DailyCounts{day, counts})
	}
	sort.Slice(result.Daily, func(i, j int) bool { return result.Daily[i].Date < result.Daily[j].Date })
	result.ReadStatus = mergeStatus(result.ReadStatus, s.readStatus())
	return result, nil
}
