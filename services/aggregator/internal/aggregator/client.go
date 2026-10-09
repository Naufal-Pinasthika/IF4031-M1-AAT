package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

type ThreatLevel string

const (
	ThreatWaspada ThreatLevel = "Waspada"
	ThreatSiaga   ThreatLevel = "Siaga"
	ThreatAwas    ThreatLevel = "Awas"
)

type AlertLevel string

const (
	AlertNormal  AlertLevel = "Normal"
	AlertWaspada AlertLevel = "Waspada"
	AlertSiaga   AlertLevel = "Siaga"
	AlertAwas    AlertLevel = "Awas"
)

type SeismicEvent struct {
	EventID          string                     `json:"event_id"`
	Magnitude        float64                    `json:"magnitude"`
	DepthKM          float64                    `json:"depth_km"`
	EpicenterLat     float64                    `json:"epicenter_lat"`
	EpicenterLon     float64                    `json:"epicenter_lon"`
	RegionName       string                     `json:"region_name"`
	OccurredAt       time.Time                  `json:"occurred_at"`
	PotentialTsunami bool                       `json:"potential_tsunami"`
	Extra            map[string]json.RawMessage `json:"-"`
}

type TsunamiWarning struct {
	WarningID        string                     `json:"warning_id"`
	RelatedEventID   string                     `json:"related_event_id"`
	ThreatLevel      ThreatLevel                `json:"threat_level"`
	AffectedZones    []string                   `json:"affected_zones"`
	EstimatedArrival time.Time                  `json:"estimated_arrival"`
	Extra            map[string]json.RawMessage `json:"-"`
}

type VolcanicReport struct {
	ReportID         string                     `json:"report_id"`
	VolcanoID        string                     `json:"volcano_id"`
	AlertLevel       AlertLevel                 `json:"alert_level"`
	EruptionCount24H int64                      `json:"eruption_count_24h"`
	AshColumnHeightM float64                    `json:"ash_column_height_m"`
	ReportedAt       time.Time                  `json:"reported_at"`
	Extra            map[string]json.RawMessage `json:"-"`
}

type Client struct {
	httpClient *http.Client
	logger     *slog.Logger
}

func NewClient(timeout time.Duration, logger *slog.Logger) *Client {
	return &Client{httpClient: &http.Client{Timeout: timeout}, logger: logger}
}

func (c *Client) FetchBMKG(ctx context.Context, baseURL, apiKey, correlationID string, since time.Time) ([]SeismicEvent, []TsunamiWarning, error) {
	seismic, err := fetchJSON[SeismicEvent](c, ctx, baseURL+"/seismic-events", "BMKG", "seismic_events", correlationID, since, func(request *http.Request) {
		request.Header.Set("X-BMKG-Key", apiKey)
		request.Header.Set("X-Correlation-ID", correlationID)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("fetch BMKG seismic events: %w", err)
	}

	warnings, err := fetchJSON[TsunamiWarning](c, ctx, baseURL+"/tsunami-warnings", "BMKG", "tsunami_warnings", correlationID, since, func(request *http.Request) {
		request.Header.Set("X-BMKG-Key", apiKey)
		request.Header.Set("X-Correlation-ID", correlationID)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("fetch BMKG tsunami warnings: %w", err)
	}
	return seismic, warnings, nil
}

func (c *Client) FetchPVMBG(ctx context.Context, baseURL, token, correlationID string, since time.Time) ([]VolcanicReport, error) {
	reports, err := fetchJSON[VolcanicReport](c, ctx, baseURL+"/volcanic-reports", "PVMBG", "volcanic_reports", correlationID, since, func(request *http.Request) {
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Correlation-ID", correlationID)
	})
	if err != nil {
		return nil, fmt.Errorf("fetch PVMBG volcanic reports: %w", err)
	}
	return reports, nil
}

func fetchJSON[T any](c *Client, ctx context.Context, endpoint, dependency, operation, correlationID string, since time.Time, configure func(*http.Request)) ([]T, error) {
	var result []T
	requestURL, err := url.Parse(endpoint)
	if err != nil {
		return result, err
	}
	query := requestURL.Query()
	query.Set("since", since.UTC().Format(time.RFC3339))
	requestURL.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return result, err
	}
	configure(request)
	startedAt := time.Now()
	response, err := c.httpClient.Do(request)
	if err != nil {
		c.logger.Error("outbound_http_failed", "dependency", dependency, "operation", operation, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds(), "error", err)
		return result, err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		c.logger.Warn("outbound_http_completed", "dependency", dependency, "operation", operation, "status", response.StatusCode, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
		return result, fmt.Errorf("source returned HTTP %d", response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		c.logger.Error("outbound_http_decode_failed", "dependency", dependency, "operation", operation, "status", response.StatusCode, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds(), "error", err)
		return result, err
	}
	c.logger.Info("outbound_http_completed", "dependency", dependency, "operation", operation, "status", response.StatusCode, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
	return result, nil
}

func (event *SeismicEvent) UnmarshalJSON(data []byte) error {
	type plainSeismicEvent SeismicEvent

	var decoded plainSeismicEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}

	*event = SeismicEvent(decoded)
	event.Extra = unknownFields(data, map[string]bool{
		"event_id": true, "magnitude": true, "depth_km": true,
		"epicenter_lat": true, "epicenter_lon": true, "region_name": true,
		"occurred_at": true, "potential_tsunami": true,
	})
	return nil
}

func (warning *TsunamiWarning) UnmarshalJSON(data []byte) error {
	type plainTsunamiWarning TsunamiWarning

	var decoded plainTsunamiWarning
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*warning = TsunamiWarning(decoded)
	warning.Extra = unknownFields(data, map[string]bool{
		"warning_id": true, "related_event_id": true, "threat_level": true,
		"affected_zones": true, "estimated_arrival": true,
	})
	return nil
}

func (report *VolcanicReport) UnmarshalJSON(data []byte) error {
	type plainVolcanicReport VolcanicReport

	var decoded plainVolcanicReport
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*report = VolcanicReport(decoded)
	report.Extra = unknownFields(data, map[string]bool{
		"report_id": true, "volcano_id": true, "alert_level": true,
		"eruption_count_24h": true, "ash_column_height_m": true,
		"reported_at": true,
	})
	return nil
}

func unknownFields(data []byte, known map[string]bool) map[string]json.RawMessage {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil
	}

	extra := make(map[string]json.RawMessage)
	for key, value := range fields {
		if !known[key] {
			extra[key] = value
		}
	}
	return extra
}
