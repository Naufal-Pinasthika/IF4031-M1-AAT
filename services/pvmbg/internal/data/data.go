package data

import (
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/model"
)

// simple mockup data
var VolcanicReports = []model.VolcanicReport{
	{
		ReportID:         "VR-001",
		VolcanoID:        "MERAPI",
		AlertLevel:       model.AlertSiaga,
		EruptionCount24H: 3,
		AshColumnHeightM: 1200,
		ReportedAt:       time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC),
	},
	{
		ReportID:         "VR-002",
		VolcanoID:        "SEMERU",
		AlertLevel:       model.AlertWaspada,
		EruptionCount24H: 1,
		AshColumnHeightM: 700,
		ReportedAt:       time.Date(2026, 9, 24, 11, 0, 0, 0, time.UTC),
	},
}