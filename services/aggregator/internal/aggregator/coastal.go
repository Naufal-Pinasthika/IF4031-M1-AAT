package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

const coastalResponseLimit = 4 << 20

var ErrCircuitOpen = store.ErrSourcePaused

type CoastalStation struct {
	StationID string  `json:"station_id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type CoastalObservation struct {
	ObservationID   string    `json:"observation_id"`
	StationID       string    `json:"station_id"`
	ObservedAt      time.Time `json:"observed_at"`
	SeaLevelAnomaly float64   `json:"sea_level_anomaly_m"`
	WaveHeight      float64   `json:"wave_height_m"`
	Quality         string    `json:"quality"`
	Critical        bool      `json:"critical"`
}

type InvalidCoastalItem struct {
	Reason  string
	Payload []byte
}

type CoastalPage struct {
	GeneratedAt time.Time
	Payload     []byte
	Items       []CoastalObservation
	Invalid     []InvalidCoastalItem
	NextCursor  string
}

type CoastalProtocolError struct {
	Reason    string
	Payload   []byte
	Retryable bool
}

func (e *CoastalProtocolError) Error() string { return e.Reason }

type CoastalDependencyError struct{ Err error }

func (e *CoastalDependencyError) Error() string { return e.Err.Error() }
func (e *CoastalDependencyError) Unwrap() error { return e.Err }

type CoastalClient struct {
	client   *http.Client
	logger   *slog.Logger
	circuit  *CircuitBreaker
	mu       sync.RWMutex
	stations map[string]CoastalStation
}

func NewCoastalClient(timeout time.Duration, failures int, openDuration time.Duration, logger *slog.Logger) *CoastalClient {
	return &CoastalClient{
		client:   sourceHTTPClient(timeout),
		logger:   logger,
		circuit:  NewCircuitBreaker(failures, openDuration),
		stations: make(map[string]CoastalStation),
	}
}

func (c *CoastalClient) CircuitState() string { return c.circuit.State() }

func (c *CoastalClient) FetchPage(ctx context.Context, baseURL, token, cursor, correlationID string, checkProgress func(context.Context, time.Time) error) (CoastalPage, error) {
	breakerNow := time.Now()
	if !c.circuit.Allow(breakerNow) {
		return CoastalPage{}, ErrCircuitOpen
	}
	if c.stationCount() == 0 {
		if err := c.fetchStations(ctx, baseURL, token, correlationID); err != nil {
			c.circuit.Failure(time.Now())
			return CoastalPage{}, err
		}
	}
	page, err := c.fetchObservations(ctx, baseURL, token, cursor, correlationID)
	if err == nil && checkProgress != nil {
		err = checkProgress(ctx, page.GeneratedAt)
		var dependencyErr *CoastalDependencyError
		if errors.As(err, &dependencyErr) {
			c.circuit.Abandon(time.Now())
			return page, err
		}
		if errors.Is(err, store.ErrSourceClockStale) || errors.Is(err, store.ErrSourceClockRegression) {
			err = &CoastalProtocolError{Reason: err.Error(), Retryable: true}
		}
		var protocolErr *CoastalProtocolError
		if errors.As(err, &protocolErr) {
			protocolErr.Payload = page.Payload
		}
	}
	if err != nil {
		// Cooldown starts when the attempt FAILS, not when a slow or timed-out
		// call started; otherwise the open interval may already have elapsed.
		c.circuit.Failure(time.Now())
		return page, err
	}
	c.circuit.Success()
	return page, nil
}

func (c *CoastalClient) fetchStations(ctx context.Context, baseURL, token, correlationID string) error {
	payload, err := c.get(ctx, strings.TrimRight(baseURL, "/")+"/v1/stations", token, correlationID, "stations")
	if err != nil {
		return err
	}
	var envelope struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return &CoastalProtocolError{Reason: "invalid_stations_json: " + err.Error(), Payload: payload}
	}
	if len(envelope.Items) == 0 || len(envelope.Items) > 10000 {
		return &CoastalProtocolError{Reason: "invalid_stations_count", Payload: payload}
	}
	stations := make(map[string]CoastalStation, len(envelope.Items))
	for _, raw := range envelope.Items {
		var station CoastalStation
		if err := decodeCoastalObject(raw, &station, "station_id", "name", "province", "latitude", "longitude"); err != nil {
			return &CoastalProtocolError{Reason: "invalid_station: " + err.Error(), Payload: raw}
		}
		if err := validateStation(station); err != nil {
			return &CoastalProtocolError{Reason: "invalid_station: " + err.Error(), Payload: raw}
		}
		if _, duplicate := stations[station.StationID]; duplicate {
			return &CoastalProtocolError{Reason: "duplicate_station_id", Payload: raw}
		}
		stations[station.StationID] = station
	}
	c.mu.Lock()
	c.stations = stations
	c.mu.Unlock()
	return nil
}

func (c *CoastalClient) fetchObservations(ctx context.Context, baseURL, token, cursor, correlationID string) (CoastalPage, error) {
	endpoint, err := url.Parse(strings.TrimRight(baseURL, "/") + "/v1/observations")
	if err != nil {
		return CoastalPage{}, err
	}
	query := endpoint.Query()
	query.Set("limit", "1000")
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	endpoint.RawQuery = query.Encode()
	payload, err := c.get(ctx, endpoint.String(), token, correlationID, "observations")
	if err != nil {
		return CoastalPage{}, err
	}
	var envelope struct {
		GeneratedAt time.Time         `json:"generated_at"`
		Items       []json.RawMessage `json:"items"`
		NextCursor  string            `json:"next_cursor"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return CoastalPage{}, &CoastalProtocolError{Reason: "invalid_observations_json: " + err.Error(), Payload: payload}
	}
	if envelope.GeneratedAt.IsZero() {
		return CoastalPage{}, &CoastalProtocolError{Reason: "missing_generated_at", Payload: payload}
	}
	if envelope.Items == nil {
		return CoastalPage{}, &CoastalProtocolError{Reason: "missing_observations_items", Payload: payload}
	}
	if len(envelope.Items) > 1000 || len(envelope.NextCursor) > 4096 {
		return CoastalPage{}, &CoastalProtocolError{Reason: "invalid_page_bounds", Payload: payload}
	}
	page := CoastalPage{GeneratedAt: envelope.GeneratedAt.UTC(), NextCursor: envelope.NextCursor, Payload: payload}
	seen := make(map[string]bool, len(envelope.Items))
	for _, raw := range envelope.Items {
		var observation CoastalObservation
		if err := decodeCoastalObject(raw, &observation, "observation_id", "station_id", "observed_at", "sea_level_anomaly_m", "wave_height_m", "quality", "critical"); err != nil {
			page.Invalid = append(page.Invalid, InvalidCoastalItem{Reason: "invalid_observation_json: " + err.Error(), Payload: bytes.Clone(raw)})
			continue
		}
		station, ok := c.station(observation.StationID)
		if err := validateObservation(observation, station, ok); err != nil {
			page.Invalid = append(page.Invalid, InvalidCoastalItem{Reason: err.Error(), Payload: bytes.Clone(raw)})
			continue
		}
		if seen[observation.ObservationID] {
			page.Invalid = append(page.Invalid, InvalidCoastalItem{Reason: "duplicate_observation_id", Payload: bytes.Clone(raw)})
			continue
		}
		seen[observation.ObservationID] = true
		page.Items = append(page.Items, observation)
	}
	return page, nil
}

func (c *CoastalClient) get(ctx context.Context, endpoint, token, correlationID, operation string) (payload []byte, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Correlation-ID", correlationID)
	started := time.Now()
	status := 0
	defer func() {
		fields := []any{"operation", operation, "status", status, "correlation_id", correlationID, "latency_ms", time.Since(started).Milliseconds()}
		if err != nil {
			c.logger.Warn("coastal_request_failed", append(fields, "error", err)...)
		} else {
			c.logger.Info("coastal_request_completed", fields...)
		}
	}()
	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	status = response.StatusCode
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, coastalResponseLimit+1)
	payload, err = io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(payload) > coastalResponseLimit {
		return nil, &CoastalProtocolError{Reason: "response_too_large", Payload: payload[:coastalResponseLimit]}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("coastal %s returned HTTP %d", operation, response.StatusCode)
	}
	return payload, nil
}

func (c *CoastalClient) stationCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.stations)
}

func (c *CoastalClient) station(id string) (CoastalStation, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	station, ok := c.stations[id]
	return station, ok
}

func validateStation(station CoastalStation) error {
	if strings.TrimSpace(station.StationID) == "" || strings.TrimSpace(station.Name) == "" || strings.TrimSpace(station.Province) == "" {
		return errors.New("station required field is empty")
	}
	if math.IsNaN(station.Latitude) || math.IsNaN(station.Longitude) || station.Latitude < -90 || station.Latitude > 90 || station.Longitude < -180 || station.Longitude > 180 {
		return errors.New("station coordinates are invalid")
	}
	return nil
}

// Zero is a legitimate measurement and false is a legitimate critical flag.
// Check presence separately so missing/null fields cannot silently become them.
func decodeCoastalObject(raw []byte, target any, required ...string) error {
	if !utf8.Valid(raw) {
		return errors.New("invalid_utf8")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("missing_required_field: %s", name)
		}
	}
	return json.Unmarshal(raw, target)
}

func validateObservation(item CoastalObservation, station CoastalStation, stationFound bool) error {
	if strings.TrimSpace(item.ObservationID) == "" || strings.TrimSpace(item.StationID) == "" {
		return errors.New("observation required identifier is empty")
	}
	if !stationFound || station.StationID != item.StationID {
		return errors.New("unknown_station_id")
	}
	// The contract requires a timestamp, not a bound against generated_at.
	// That independent clock may itself be stalled/regressed; do not discard
	// a structurally valid critical alert using an invented skew allowance.
	if item.ObservedAt.IsZero() {
		return errors.New("invalid_observed_at")
	}
	if strings.TrimSpace(item.Quality) == "" || math.IsNaN(item.SeaLevelAnomaly) || math.IsInf(item.SeaLevelAnomaly, 0) || math.IsNaN(item.WaveHeight) || math.IsInf(item.WaveHeight, 0) || item.WaveHeight < 0 {
		return errors.New("invalid_observation_value")
	}
	return nil
}

func mapCoastal(item CoastalObservation, station CoastalStation) model.HazardEvent {
	severity := model.SeverityNORMAL
	if item.Critical {
		severity = model.SeverityAWAS
	}
	return model.HazardEvent{
		Source:      model.SourceSENSORPANTAI,
		SourceRefID: item.ObservationID,
		HazardType:  model.HazardTypeCOASTAL,
		Severity:    severity,
		AreaName:    station.Province,
		Latitude:    station.Latitude,
		Longitude:   station.Longitude,
		OccuredAt:   item.ObservedAt.UTC(),
		Attributes: map[string]any{
			"station_id": item.StationID, "station_name": station.Name,
			"sea_level_anomaly_m": item.SeaLevelAnomaly, "wave_height_m": item.WaveHeight,
			"quality": item.Quality, "critical": item.Critical,
		},
	}
}
