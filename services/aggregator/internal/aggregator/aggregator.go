package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/broker"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	metric "github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/metrics"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/status"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

// p1 delayed tsunami warnings look up the already persisted parent quake.
type EventStore interface {
	GetEventBySourceRef(context.Context, string, string) (store.EventLookup, error) // p1
	UpsertAccepted(context.Context, *model.HazardEvent, int64) error
	UpsertAcceptedTarget(context.Context, *model.HazardEvent, int64, string) error
}

type SourceCoordinator interface {
	AcquireOrRenewLease(context.Context, string, string, time.Duration) (store.Lease, bool, error)
	RenewLease(context.Context, string, string, int64, time.Duration) error
	ReleaseLease(context.Context, string, string, int64) error
	ValidateLease(context.Context, string, string, int64) error
	BeginPoll(context.Context, string, string, int64, time.Duration) (store.PollAttempt, bool, error)
	ValidatePoll(context.Context, string, string, int64, int64) error
	FinishPoll(context.Context, store.PollAttempt, time.Duration, error) error
	AcceptEvent(context.Context, string, string, int64, *model.HazardEvent, string) error
	AcceptBatch(context.Context, string, string, int64, []model.HazardEvent, string) error
	PendingEvent(context.Context, string, string) (*model.HazardEvent, error)
	DeliverNext(context.Context, string, string, func(context.Context, store.AcceptedEvent) error) (bool, error)
	TimeCursor(context.Context, string, time.Time) (time.Time, error)
	OpaqueCursor(context.Context, string) (string, error)
	AdvanceTimeCursor(context.Context, string, string, int64, time.Time) error
	AdvanceOpaqueCursor(context.Context, string, string, int64, string) error
	Quarantine(context.Context, string, string, []byte, bool) error
	Now(context.Context) (time.Time, error)
	CheckSourceProgress(context.Context, string, time.Time, time.Duration) error
}

type EventPublisher interface {
	Publish(ctx context.Context, envelope broker.EventEnvelope) error
}

type Aggregator struct {
	poller      *Poller
	satellite   *SatelliteConsumer
	store       EventStore
	publisher   EventPublisher
	config      config.Config
	logger      *slog.Logger
	status      *status.Tracker
	coordinator SourceCoordinator
	coastal     *CoastalClient
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

func NewAggregator(poller *Poller, satellite *SatelliteConsumer, eventStore EventStore, coordinator SourceCoordinator, publisher EventPublisher, cfg config.Config, logger *slog.Logger, tracker *status.Tracker) *Aggregator {
	return &Aggregator{
		poller:      poller,
		satellite:   satellite,
		store:       eventStore,
		coordinator: coordinator,
		publisher:   publisher,
		config:      cfg,
		logger:      logger,
		status:      tracker,
		coastal:     NewCoastalClient(cfg.CoastalTimeout, cfg.CircuitFailures, cfg.CircuitOpenDuration, logger),
	}
}

func (a *Aggregator) Run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Go(func() { a.runBMKG(ctx) })
	workers.Go(func() { a.runPVMBG(ctx) })
	if a.config.SatelliteToken != "" {
		workers.Go(func() { a.runSatellite(ctx) })
	}
	if a.config.CoastalToken != "" {
		workers.Go(func() { a.runCoastal(ctx) })
	}
	workers.Wait()
}

func (a *Aggregator) runSatellite(ctx context.Context) {
	c, ok := a.coordinator.(*store.Coordinator)
	if !ok {
		return
	}
	for ctx.Err() == nil {
		lease, leader, err := c.AcquireOrRenewLease(ctx, "SATELIT", a.config.InstanceID, a.config.LeaseTTL)
		if err == nil && leader {
			cycle, stop := a.leaseContext(ctx, "SATELIT", lease.Token)
			observed := make(chan struct{})
			go func() {
				defer close(observed)
				for cycle.Err() == nil {
					probe, cancel := context.WithTimeout(cycle, 4*time.Second)
					session, err := a.satellite.stream.Session(probe)
					if err == nil {
						_ = c.ObserveSatelliteSession(probe, session.LatestOffset)
					}
					cancel()
					timer := time.NewTimer(5 * time.Second)
					select {
					case <-cycle.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}()
			offset, loadErr := c.SatelliteOffset(cycle)
			if loadErr == nil {
				loadErr = a.satellite.ConsumeBatches(cycle, offset, func(events []SatelliteStreamEvent) error {
					batch := make([]model.SatelliteMutation, len(events))
					for i, event := range events {
						batch[i] = model.SatelliteMutation{Type: event.Type, Payload: event.Data, Offset: event.Offset, ReceivedAt: event.ReceivedAt}
					}
					err := c.ProcessSatelliteBatch(cycle, a.config.InstanceID, lease.Token, batch)
					if err == nil {
						a.status.Success("SATELIT", time.Now())
					}
					return err
				})
			}
			stop()
			<-observed
			release, done := context.WithTimeout(context.Background(), a.config.CoordinationTimeout)
			_ = c.ReleaseLease(release, "SATELIT", a.config.InstanceID, lease.Token)
			done()
			if loadErr != nil && ctx.Err() == nil {
				a.recordPollFailure("SATELIT", loadErr)
			}
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (a *Aggregator) runBMKG(ctx context.Context) {
	a.runSource(ctx, "BMKG", a.config.PollingInterval, func(cycle context.Context, token int64) error {
		since, err := a.coordinator.TimeCursor(cycle, "BMKG", a.config.StartSince)
		if err != nil {
			return err
		}
		batch, err := a.poller.PollBMKG(cycle, since)
		if err == nil {
			batch.Events = seismicEventsAfter(batch.Events, since)
			err = a.processBMKG(cycle, batch, token)
		}
		return err
	})
}

func (a *Aggregator) runPVMBG(ctx context.Context) {
	a.runSource(ctx, "PVMBG", a.config.PollingInterval, func(cycle context.Context, token int64) error {
		return a.pollPVMBGInbox(cycle, token)
	})
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
func (a *Aggregator) processBMKG(ctx context.Context, batch BMKGBatch, token int64) error {
	warningsByEventID := make(map[string]*TsunamiWarning)
	var newest time.Time

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
		if err := a.writeFenced(operationCtx, "BMKG", token, &hazard); err != nil {
			return err
		}
		if event.OccurredAt.After(newest) {
			newest = event.OccurredAt
		}
	}

	// p1 preserve each warning identity, including warnings arriving after the quake batch
	for _, warning := range batch.Warnings {
		parent, err := a.coordinator.PendingEvent(ctx, "BMKG", warning.RelatedEventID)
		if err != nil {
			return err
		}
		if parent == nil {
			lookup, err := a.store.GetEventBySourceRef(ctx, "BMKG", warning.RelatedEventID)
			if err != nil {
				return err
			}
			parent = lookup.Event
		}
		if parent == nil {
			return fmt.Errorf("parent quake for warning %s is unavailable", warning.WarningID)
		}
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
		if err = a.writeFenced(operationCtx, "BMKG", token, &hazard); err != nil {
			return err
		}
	}
	if !newest.IsZero() {
		if err := a.coordinator.AdvanceTimeCursor(ctx, "BMKG", a.config.InstanceID, token, newest); err != nil {
			return err
		}
	}

	a.logger.Info("bmkg_batch_received", "events", len(batch.Events), "warnings", len(batch.Warnings), "correlation_id", batch.CorrelationID)
	return nil
}

func (a *Aggregator) processPVMBG(ctx context.Context, batch PVMBGBatch, token int64) error {
	var newest time.Time
	for _, report := range batch.Reports {
		hazard, err := mapVolcanic(report)
		if err != nil {
			return err
		}

		operationCtx := correlation.WithID(ctx, batch.CorrelationID)
		if err := a.writeFenced(operationCtx, "PVMBG", token, &hazard); err != nil {
			return err
		}
		if report.ReportedAt.After(newest) {
			newest = report.ReportedAt
		}
	}
	if !newest.IsZero() {
		if err := a.coordinator.AdvanceTimeCursor(ctx, "PVMBG", a.config.InstanceID, token, newest); err != nil {
			return err
		}
	}

	a.logger.Info("pvmbg_batch_received", "reports", len(batch.Reports), "correlation_id", batch.CorrelationID)
	return nil
}

func (a *Aggregator) runCoastal(ctx context.Context) {
	a.runSource(ctx, "SENSOR_PANTAI", a.config.CoastalInterval, func(cycle context.Context, token int64) error {
		started := time.Now()
		cursor, err := a.coordinator.OpaqueCursor(cycle, "SENSOR_PANTAI")
		correlationID := correlation.ID(cycle)
		if correlationID == "" {
			correlationID = newCorrelationID()
		}
		var page CoastalPage
		if err == nil {
			page, err = a.coastal.FetchPage(cycle, a.config.CoastalURL, a.config.CoastalToken, cursor, correlationID, func(ctx context.Context, generatedAt time.Time) error {
				progressErr := a.coordinator.CheckSourceProgress(ctx, "SENSOR_PANTAI", generatedAt, a.config.CoastalMaxStaleness)
				if errors.Is(progressErr, store.ErrSourceClockStale) || errors.Is(progressErr, store.ErrSourceClockRegression) {
					return &CoastalProtocolError{Reason: progressErr.Error(), Retryable: true}
				}
				return progressErr
			})
		}
		err = a.handleCoastalPage(cycle, page, err, correlationID, token)
		if err == nil {
			metric.Default.CoastalLatency(time.Since(started))
		}
		metric.Default.Circuit(a.coastal.CircuitState())
		a.status.Circuit("SENSOR_PANTAI", a.coastal.CircuitState())
		return err
	})
}

// Freshness is source health, not a schema violation in every item. Preserve
// valid alerts even when the source clock is unhealthy, without recording a
// successful poll or closing its breaker. All item validation/quarantine and
// lease-fenced durable admission still apply before checkpoint advancement.
func (a *Aggregator) handleCoastalPage(ctx context.Context, page CoastalPage, fetchErr error, correlationID string, token int64) error {
	if fetchErr == nil {
		return a.processCoastal(ctx, page, correlationID, token)
	}
	var protocolErr *CoastalProtocolError
	if !errors.As(fetchErr, &protocolErr) {
		return fetchErr
	}
	if err := a.coordinator.Quarantine(ctx, "SENSOR_PANTAI", protocolErr.Reason, protocolErr.Payload, protocolErr.Retryable); err != nil {
		return errors.Join(fetchErr, err)
	}
	clockFailure := protocolErr.Reason == store.ErrSourceClockStale.Error() || protocolErr.Reason == store.ErrSourceClockRegression.Error()
	if clockFailure && protocolErr.Retryable && len(page.Payload) != 0 {
		return errors.Join(fetchErr, a.processCoastal(ctx, page, correlationID, token))
	}
	return fetchErr
}

func (a *Aggregator) processCoastal(ctx context.Context, page CoastalPage, correlationID string, token int64) error {
	metric.Default.Invalid(len(page.Invalid))
	for _, invalid := range page.Invalid {
		if err := a.coordinator.Quarantine(ctx, "SENSOR_PANTAI", invalid.Reason, invalid.Payload, false); err != nil {
			return err
		}
	}
	// Prioritize alerts inside a burst without skipping any normal record or
	// advancing the source cursor until the complete page has been admitted.
	sort.SliceStable(page.Items, func(i, j int) bool { return page.Items[i].Critical && !page.Items[j].Critical })
	hazards := make([]model.HazardEvent, 0, len(page.Items))
	for _, observation := range page.Items {
		station, ok := a.coastal.station(observation.StationID)
		if !ok {
			return fmt.Errorf("coastal station disappeared: %s", observation.StationID)
		}
		hazards = append(hazards, mapCoastal(observation, station))
		// occurred_at belongs to the sensor's independent (possibly accelerated)
		// clock. Never subtract it from a host clock to claim an end-to-end SLO.
		if observation.Critical {
			metric.Default.Critical()
		}
	}
	operationCtx := correlation.WithID(ctx, correlationID)
	if err := a.coordinator.AcceptBatch(operationCtx, "SENSOR_PANTAI", a.config.InstanceID, token, hazards, correlationID); err != nil {
		return err
	}
	if page.NextCursor != "" {
		if err := a.coordinator.AdvanceOpaqueCursor(ctx, "SENSOR_PANTAI", a.config.InstanceID, token, page.NextCursor); err != nil {
			return err
		}
	}
	a.logger.Info("coastal_batch_received", "events", len(page.Items), "invalid", len(page.Invalid), "correlation_id", correlationID)
	return nil
}

func (a *Aggregator) writeFenced(ctx context.Context, source string, token int64, event *model.HazardEvent) error {
	err := a.coordinator.AcceptEvent(ctx, source, a.config.InstanceID, token, event, correlation.ID(ctx))
	if err != nil {
		a.logger.Warn("source_event_rejected", "source", source, "source_ref_id", event.SourceRefID,
			"holder_id", a.config.InstanceID, "fencing_token", token, "correlation_id", correlation.ID(ctx), "error", err)
		return err
	}
	a.logger.Info("source_event_accepted", "source", source, "source_ref_id", event.SourceRefID,
		"holder_id", a.config.InstanceID, "fencing_token", token, "correlation_id", correlation.ID(ctx))
	return nil
}

func (a *Aggregator) recordPollFailure(source string, err error) {
	a.status.Failure(source, time.Now())
	metric.Default.Poll(source, "failure")
	a.logger.Warn("source_poll_failed", "source", source, "error", err)
}

func (a *Aggregator) publish(ctx context.Context, event model.HazardEvent, correlationID string, version int64) error {
	if a.publisher == nil {
		return fmt.Errorf("event publisher is not configured")
	}

	envelope := broker.EventEnvelope{
		EventType:        "hazard.event.upserted",
		SchemaVersion:    1,
		CorrelationID:    correlationID,
		PublishedAt:      time.Now().UTC(),
		Data:             event,
		IngestionVersion: version,
	}
	if removed, _ := event.Attributes["_retracted"].(bool); removed {
		envelope.EventType = "hazard.event.deleted"
	}
	startedAt := time.Now()
	err := a.publisher.Publish(ctx, envelope)
	latency := time.Since(startedAt).Milliseconds()
	if err != nil {
		a.logger.Error("hazard_event_publish_failed", "hazard_id", event.HazardID, "correlation_id", correlationID, "latency_ms", latency, "error", err)
		return err
	}
	a.logger.Info("hazard_event_published", "hazard_id", event.HazardID, "correlation_id", correlationID, "latency_ms", latency)
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
