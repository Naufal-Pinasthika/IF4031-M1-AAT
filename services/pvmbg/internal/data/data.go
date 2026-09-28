package data

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/model"
)

// mock pvmbg data
var dataMu sync.RWMutex

var VolcanicReports = seedVolcanicReports()

func seedVolcanicReports() []model.VolcanicReport {
	reports := make([]model.VolcanicReport, 0, 20)

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

	volcanoes := []string{
		"MERAPI",
		"SEMERU",
		"ANAK_KRAKATAU",
		"AGUNG",
		"RINJANI",
		"LEWOTOBI",
		"MARAPI",
		"KERINCI",
		"IBU",
		"DUKONO",
	}

	alertLevels := []model.AlertLevel{
		model.AlertNormal,
		model.AlertWaspada,
		model.AlertSiaga,
		model.AlertAwas,
	}

	for i := 0; i < 20; i++ {
		reports = append(reports, model.VolcanicReport{
			ReportID:          fmt.Sprintf("VR-%03d", i+1),
			VolcanoID:         volcanoes[i%len(volcanoes)],
			AlertLevel:        alertLevels[i%len(alertLevels)],
			EruptionCount24H:  int64(i % 6),
			AshColumnHeightM:  float64(300 + (i%8)*150),
			ReportedAt:        baseTime.Add(time.Duration(i) * 12 * time.Hour),

			ConfidenceLevel: nil,
		})
	}

	return reports
}

func ListVolcanicReports(since time.Time) []model.VolcanicReport {
	dataMu.RLock()
	defer dataMu.RUnlock()

	result := make([]model.VolcanicReport, 0)

	for _, report := range VolcanicReports {
		if !report.ReportedAt.Before(since) {
			result = append(result, report)
		}
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
			appendGeneratedReport(now.UTC())

		case <-ctx.Done():
			return
		}
	}
}

func appendGeneratedReport(reportedAt time.Time) {
	dataMu.Lock()
	defer dataMu.Unlock()

	index := len(VolcanicReports) + 1

	alertLevels := []model.AlertLevel{
		model.AlertNormal,
		model.AlertWaspada,
		model.AlertSiaga,
		model.AlertAwas,
	}

	VolcanicReports = append(VolcanicReports, model.VolcanicReport{
		ReportID:          fmt.Sprintf("VR-%03d", index),
		VolcanoID:         "MERAPI",
		AlertLevel:        alertLevels[(index-1)%len(alertLevels)],
		EruptionCount24H:  int64(index % 6),
		AshColumnHeightM:  float64(300 + (index%8)*150),
		ReportedAt:        reportedAt,
		ConfidenceLevel:  nil,
	})
}