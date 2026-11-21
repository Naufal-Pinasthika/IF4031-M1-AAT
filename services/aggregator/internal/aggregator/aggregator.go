package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/status"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

// p1 delayed tsunami warnings look up the already persisted parent quake.
type EventStore interface {
	GetEventBySourceRef(context.Context, string, string) (store.EventLookup, error) // p1
	Upsert(ctx context.Context, event *model.HazardEvent) error
	ProcessSatelliteEvent(ctx context.Context, eventType string, payload []byte, offset int64) ([]model.HazardEvent, []model.HazardEvent, error)
	SatelliteOffset(ctx context.Context) (int64, error)
}

type Aggregator struct {
	poller    *Poller
	satellite *SatelliteConsumer
	store     EventStore
	config    config.Config
	logger    *slog.Logger
	status    *status.Tracker
}

type volcanoReference struct {
	Name      string
	Latitude  float64
	Longitude float64
}

var volcanoReferences = map[string]volcanoReference{
	"MERAPI":        {Name: "Gunung Merapi", Latitude: -7.54130669258, Longitude: 110.44627086700},
	"SEMERU":        {Name: "Gunung Semeru", Latitude: -8.11284536216, Longitude: 112.92476270100},
	"ANAK_KRAKATAU": {Name: "Gunung Anak Krakatau", Latitude: -6.10281411180, Longitude: 105.42279349400},
	"AGUNG":         {Name: "Gunung Agung", Latitude: -8.34397911664, Longitude: 115.50748172600},
	"RINJANI":       {Name: "Gunung Rinjani", Latitude: -8.41111052321, Longitude: 116.42315870100},
	"LEWOTOBI":      {Name: "Gunung Lewotobi", Latitude: -8.53809642487, Longitude: 122.76735601700},
	"MARAPI":        {Name: "Gunung Marapi", Latitude: -0.39106938431, Longitude: 100.45709783400},
	"KERINCI":       {Name: "Gunung Kerinci", Latitude: -1.69600133075, Longitude: 101.26719098600},
	"IBU":           {Name: "Gunung Ibu", Latitude: 1.488, Longitude: 127.630},
	"DUKONO":        {Name: "Gunung Dukono", Latitude: 1.69867327144, Longitude: 127.87801665200},
}

func NewAggregator(poller *Poller, satellite *SatelliteConsumer, store EventStore, cfg config.Config, logger *slog.Logger, tracker *status.Tracker) *Aggregator {
	return &Aggregator{
		poller:    poller,
		satellite: satellite,
		store:     store,
		config:    cfg,
		logger:    logger,
		status:    tracker,
	}
}

func (a *Aggregator) Run(ctx context.Context) {
	go a.runBMKG(ctx)
	go a.runPVMBG(ctx)
	go a.runSatellite(ctx)
	<-ctx.Done()
}

func (a *Aggregator) runSatellite(ctx context.Context) {
	fromOffset, err := a.store.SatelliteOffset(ctx)
	if err != nil {
		a.logger.Error("satellite_offset_load_failed", "error", err)
		return
	}
	err = a.satellite.Consume(ctx, fromOffset, func(event SatelliteStreamEvent) error {
		events, deleted, err := a.store.ProcessSatelliteEvent(ctx, event.Type, event.Data, event.Offset)
		if err != nil {
			return err
		}
		for i := range events {
			_ = events[i]
		}
		for i := range deleted {
			_ = deleted[i]
		}
		a.status.Success("SATELIT", time.Now())
		return nil
	})
	if err != nil && ctx.Err() == nil {
		a.logger.Error("satellite_feed_failed", "error", err)
	}
}

func (a *Aggregator) runBMKG(ctx context.Context) {
	since := a.config.StartSince
	ticker := time.NewTicker(a.config.PollingInterval)
	defer ticker.Stop()

	for {
		batch, err := a.poller.PollBMKG(ctx, since)
		if err == nil {
			batch.Events = seismicEventsAfter(batch.Events, since)
			err = a.processBMKG(ctx, batch)
			if err == nil {
				for _, event := range batch.Events {
					if event.OccurredAt.After(since) {
						since = event.OccurredAt
					}
				}
			}
		}
		// mark source after data saved to store
		if err == nil {
			a.status.Success("BMKG", time.Now())
		} else if ctx.Err() == nil {
			a.status.Failure("BMKG", time.Now())
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
			batch.Reports = volcanicReportsAfter(batch.Reports, since)
			err = a.processPVMBG(ctx, batch)
			if err == nil {
				for _, report := range batch.Reports {
					if report.ReportedAt.After(since) {
						since = report.ReportedAt
					}
				}
			}
		}
		// mark source after data saved to store
		if err == nil {
			a.status.Success("PVMBG", time.Now())
		} else if ctx.Err() == nil {
			a.status.Failure("PVMBG", time.Now())
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func seismicEventsAfter(events []SeismicEvent, since time.Time) []SeismicEvent {
	filtered := make([]SeismicEvent, 0, len(events))
	for _, event := range events {
		if event.OccurredAt.After(since) {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func volcanicReportsAfter(reports []VolcanicReport, since time.Time) []VolcanicReport {
	filtered := make([]VolcanicReport, 0, len(reports))
	for _, report := range reports {
		if report.ReportedAt.After(since) {
			filtered = append(filtered, report)
		}
	}
	return filtered
}

// p1
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

		operationCtx := correlation.WithID(ctx, batch.CorrelationID)
		if err := a.store.Upsert(operationCtx, &hazard); err != nil {
			return err
		}
	}

	// p1 preserve each warning identity, including warnings arriving after the quake batch
	for _, warning := range batch.Warnings {
		lookup, err := a.store.GetEventBySourceRef(ctx, "BMKG", warning.RelatedEventID)
		if err != nil {
			return err
		}
		if lookup.Event == nil {
			return fmt.Errorf("parent quake for warning %s is unavailable", warning.WarningID)
		}
		parent := lookup.Event
		hazard := model.HazardEvent{Source: model.SourceBMKG, SourceRefID: warning.WarningID, HazardType: model.HazardTypeTSUNAMI, AreaName: parent.AreaName, Latitude: parent.Latitude, Longitude: parent.Longitude, OccuredAt: parent.OccuredAt,
			Attributes: map[string]any{"warning_id": warning.WarningID, "related_event_id": warning.RelatedEventID, "threat_level": warning.ThreatLevel, "affected_zones": warning.AffectedZones, "estimated_arrival": warning.EstimatedArrival}}
		switch warning.ThreatLevel {
		case ThreatAwas:
			hazard.Severity = model.SeverityAWAS
		case ThreatSiaga:
			hazard.Severity = model.SeveritySIAGA
		case ThreatWaspada:
			hazard.Severity = model.SeverityWASPADA
		default:
			return fmt.Errorf("unknown tsunami threat level")
		}
		operationCtx := correlation.WithID(ctx, batch.CorrelationID)
		if err = a.store.Upsert(operationCtx, &hazard); err != nil {
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

		operationCtx := correlation.WithID(ctx, batch.CorrelationID)
		if err := a.store.Upsert(operationCtx, &hazard); err != nil {
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
	reference, ok := volcanoReferences[report.VolcanoID]
	if !ok {
		return model.HazardEvent{}, fmt.Errorf("unknown volcano_id %q", report.VolcanoID)
	}

	result := model.HazardEvent{
		Source:      model.SourcePVMBG,
		SourceRefID: report.ReportID,
		HazardType:  model.HazardTypeVOLCANIC,
		AreaName:    reference.Name,
		Latitude:    reference.Latitude,
		Longitude:   reference.Longitude,
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
	attributes := map[string]any{
		"eruption_count_24h":  report.EruptionCount24H,
		"ash_column_height_m": report.AshColumnHeightM,
	}
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
