package data

import (
	"fmt"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/model"
)

// mock pvmbg data
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