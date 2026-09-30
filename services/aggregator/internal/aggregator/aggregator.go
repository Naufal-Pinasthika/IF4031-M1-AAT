package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

type EventStore interface {
	Upsert(ctx context.Context, event *model.HazardEvent) error
}

type Aggregator struct {
	poller *Poller
	store  EventStore
	config config.Config
	logger *slog.Logger
}

func NewAggregator(poller *Poller, store EventStore, cfg config.Config, logger *slog.Logger) *Aggregator {
	return &Aggregator{poller: poller, store: store, config: cfg, logger: logger}
}

func (a *Aggregator) Run(ctx context.Context) {
	go a.runBMKG(ctx)
	go a.runPVMBG(ctx)
	<-ctx.Done()
}

func (a *Aggregator) runBMKG(ctx context.Context) {
	since := a.config.StartSince
	ticker := time.NewTicker(a.config.PollingInterval)
	defer ticker.Stop()

	for {
		batch, err := a.poller.PollBMKG(ctx, since)
		if err == nil {
			if err := a.processBMKG(ctx, batch); err == nil {
				for _, event := range batch.Events {
					if event.OccurredAt.After(since) {
						since = event.OccurredAt
					}
				}
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (a *Aggregator) runPVMBG(ctx context.Context) {
	since := a.config.StartSince
	ticker := time.NewTicker(a.config.PollingInterval)
	defer ticker.Stop()

	for {
		batch, err := a.poller.PollPVMBG(ctx, since)
		if err == nil {
			if err := a.processPVMBG(ctx, batch); err == nil {
				for _, report := range batch.Reports {
					if report.ReportedAt.After(since) {
						since = report.ReportedAt
					}
				}
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (a *Aggregator) processBMKG(ctx context.Context, batch BMKGBatch) error {
	warningsByEventID := make(map[string]*TsunamiWarning)

	for index := range batch.Warnings {
		warning := &batch.Warnings[index]
		warningsByEventID[warning.RelatedEventID] = warning
	}

	for _, event := range batch.Events {
		hazard, err := mapSeismic(event, warningsByEventID[event.EventID])
		if err != nil {
			return err
		}

		if err := a.store.Upsert(ctx, &hazard); err != nil {
			return err
		}
	}

	a.logger.Info("bmkg_batch_received", "events", len(batch.Events), "warnings", len(batch.Warnings), "correlation_id", batch.CorrelationID)
	return nil
}

func (a *Aggregator) processPVMBG(ctx context.Context, batch PVMBGBatch) error {
	for _, report := range batch.Reports {
		hazard, err := mapVolcanic(report)
		if err != nil {
			return err
		}

		if err := a.store.Upsert(ctx, &hazard); err != nil {
			return err
		}
	}

	a.logger.Info("pvmbg_batch_received", "reports", len(batch.Reports), "correlation_id", batch.CorrelationID)
	return nil
}

func mapSeismic(event SeismicEvent, warning *TsunamiWarning) (model.HazardEvent, error) {
	result := model.HazardEvent{
		Source:      model.SourceBMKG,
		SourceRefID: event.EventID,
		HazardType:  model.HazardTypeSEISMIC,
		AreaName:    event.RegionName,
		Latitude:    event.EpicenterLat,
		Longitude:   event.EpicenterLon,
		OccuredAt:   event.OccurredAt,
	}

	// Map severity
	if warning != nil {
		switch warning.ThreatLevel {
		case ThreatAwas:
			result.Severity = model.SeverityAWAS
		case ThreatSiaga:
			result.Severity = model.SeveritySIAGA
		case ThreatWaspada:
			result.Severity = model.SeverityWASPADA
		}
	} else {
		switch {
		case event.Magnitude < 5.0:
			result.Severity = model.SeverityNORMAL
		case event.Magnitude >= 5.0 && event.Magnitude < 6.5:
			result.Severity = model.SeverityWASPADA
		default: // event.Magnitude >= 6.5
			result.Severity = model.SeveritySIAGA
		}
	}

	// Map extra attributes
	attributes := map[string]any{
		"magnitude":         event.Magnitude,
		"depth_km":          event.DepthKM,
		"potential_tsunami": event.PotentialTsunami,
	}
	for key, rawValue := range event.Extra {
		var value any
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return model.HazardEvent{}, fmt.Errorf("decode extra attribute %q: %w", key, err)
		}
		attributes[key] = value
	}
	if warning != nil {
		attributes["warning_id"] = warning.WarningID
		attributes["related_event_id"] = warning.RelatedEventID
		attributes["threat_level"] = warning.ThreatLevel
		attributes["affected_zones"] = warning.AffectedZones
		attributes["estimated_arrival"] = warning.EstimatedArrival
		for key, rawValue := range warning.Extra {
			var value any
			if err := json.Unmarshal(rawValue, &value); err != nil {
				return model.HazardEvent{}, fmt.Errorf("decode extra attribute %q: %w", key, err)
			}
			attributes[key] = value
		}
	}

	result.Attributes = attributes

	return result, nil
}

func mapVolcanic(report VolcanicReport) (model.HazardEvent, error) {
	volcanoPos := map[string][2]float64{
		"MERAPI":        {-7.54130669258, 110.44627086700},
		"SEMERU":        {-8.11284536216, 112.92476270100},
		"ANAK_KRAKATAU": {-6.10281411180, 105.42279349400},
		"AGUNG":         {-8.34397911664, 115.50748172600},
		"RINJANI":       {-8.41111052321, 116.42315870100},
		"LEWOTOBI":      {-8.53809642487, 122.76735601700},
		"MARAPI":        {-0.39106938431, 100.45709783400},
		"KERINCI":       {-1.69600133075, 101.26719098600},
		"IBU":           {1.488, 127.630},
		"DUKONO":        {1.69867327144, 127.87801665200},
	}

	result := model.HazardEvent{
		Source:      model.SourcePVMBG,
		SourceRefID: report.ReportID,
		HazardType:  model.HazardTypeVOLCANIC,
		AreaName:    report.VolcanoID,
		Latitude:    volcanoPos[report.VolcanoID][0],
		Longitude:   volcanoPos[report.VolcanoID][1],
		OccuredAt:   report.ReportedAt,
	}

	// Map severity
	switch report.AlertLevel {
	case AlertNormal:
		result.Severity = model.SeverityNORMAL
	case AlertWaspada:
		result.Severity = model.SeverityWASPADA
	case AlertSiaga:
		result.Severity = model.SeveritySIAGA
	case AlertAwas:
		result.Severity = model.SeverityAWAS
	}

	// Map extra attributes
	attributes := make(map[string]any)
	for key, rawValue := range report.Extra {
		var value any
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return model.HazardEvent{}, fmt.Errorf("decode extra attribute %q: %w", key, err)
		}
		attributes[key] = value
	}

	result.Attributes = attributes

	return result, nil
}
