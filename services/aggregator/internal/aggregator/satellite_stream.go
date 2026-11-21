package aggregator

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type SatelliteSession struct {
	Group        string `json:"group"`
	Profile      string `json:"profile"`
	Speed        int64  `json:"speed"`
	VirtualNow   string `json:"virtual_now"`
	LatestOffset int64  `json:"latest_offset"`
}

type SatelliteStreamEvent struct {
	Type       string
	Offset     int64
	Data       json.RawMessage
	ReceivedAt time.Time
}

type SatelliteStream struct {
	baseURL string
	token   string
	client  *http.Client
	logger  *slog.Logger
}

func NewSatelliteStream(baseURL, token string, logger *slog.Logger) *SatelliteStream {
	return &SatelliteStream{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		client: &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ResponseHeaderTimeout: 10 * time.Second, TLSHandshakeTimeout: 10 * time.Second}},
		logger: logger,
	}
}

func (f *SatelliteStream) Run(ctx context.Context, fromOffset int64, handler func(SatelliteStreamEvent) error) error {
	if f.token == "" {
		return fmt.Errorf("SATELLITE_TOKEN is required")
	}

	offset := fromOffset
	backoff := time.Second
	for ctx.Err() == nil {
		nextOffset, err := f.consume(ctx, offset, handler)
		if nextOffset > offset {
			offset = nextOffset
			backoff = time.Second
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			f.logger.Warn("satellite_stream_disconnected", "offset", offset, "backoff_ms", backoff.Milliseconds(), "error", err)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff = min(30*time.Second, backoff*2)
	}
	return nil
}

func (f *SatelliteStream) Session(ctx context.Context) (SatelliteSession, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.baseURL+"/v1/session", nil)
	if err != nil {
		return SatelliteSession{}, err
	}
	f.setHeaders(request)

	response, err := f.client.Do(request)
	if err != nil {
		return SatelliteSession{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return SatelliteSession{}, satelliteStatusError(response)
	}

	var session SatelliteSession
	if err := json.NewDecoder(response.Body).Decode(&session); err != nil {
		return SatelliteSession{}, fmt.Errorf("decode satellite session: %w", err)
	}
	return session, nil
}

func (f *SatelliteStream) consume(ctx context.Context, fromOffset int64, handler func(SatelliteStreamEvent) error) (int64, error) {
	streamURL, err := url.Parse(f.baseURL + "/v1/stream")
	if err != nil {
		return fromOffset, err
	}
	query := streamURL.Query()
	query.Set("from", strconv.FormatInt(fromOffset+1, 10))
	streamURL.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL.String(), nil)
	if err != nil {
		return fromOffset, err
	}
	f.setHeaders(request)
	request.Header.Set("Accept", "text/event-stream")
	if fromOffset >= 0 {
		request.Header.Set("Last-Event-ID", strconv.FormatInt(fromOffset, 10))
	}

	response, err := f.client.Do(request)
	if err != nil {
		return fromOffset, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fromOffset, satelliteStatusError(response)
	}
	idle := time.AfterFunc(30*time.Second, func() { _ = response.Body.Close() })
	defer idle.Stop()

	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	eventType := ""
	eventID := ""
	var dataLines []string
	frameBytes := 0

	for scanner.Scan() {
		idle.Reset(30 * time.Second)
		line := scanner.Text()
		if line == "" {
			if len(dataLines) == 0 {
				eventType = ""
				eventID = ""
				frameBytes = 0
				continue
			}
			event, err := buildSatelliteEvent(eventType, eventID, dataLines)
			event.ReceivedAt = time.Now()
			eventType = ""
			eventID = ""
			dataLines = dataLines[:0]
			frameBytes = 0
			if err != nil {
				return fromOffset, err
			}
			if event.Type == "heartbeat" && event.Offset < 0 {
				continue
			} // Only unsequenced transport keepalives have no durable effect.
			if handler != nil {
				if err := handler(event); err != nil {
					return fromOffset, err
				}
			}
			if event.Offset > fromOffset {
				fromOffset = event.Offset
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			eventType = value
		case "id":
			eventID = value
		case "data":
			frameBytes += len(value) + 1
			if frameBytes > 1<<20 {
				return fromOffset, fmt.Errorf("satellite SSE frame exceeds 1 MiB")
			}
			dataLines = append(dataLines, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return fromOffset, fmt.Errorf("read satellite SSE: %w", err)
	}
	return fromOffset, fmt.Errorf("satellite stream ended")
}

func buildSatelliteEvent(eventType, eventID string, dataLines []string) (SatelliteStreamEvent, error) {
	payload := strings.Join(dataLines, "\n")
	offset, err := strconv.ParseInt(eventID, 10, 64)
	if err != nil {
		offset = -1
		var envelope struct {
			Offset *int64 `json:"offset"`
		}
		if payload != "" && json.Unmarshal([]byte(payload), &envelope) == nil && envelope.Offset != nil {
			offset = *envelope.Offset
		}
	}
	if offset < 0 && eventType != "heartbeat" {
		return SatelliteStreamEvent{}, fmt.Errorf("satellite event %q has no numeric offset", eventType)
	}
	if eventType == "" {
		eventType = "message"
	}
	return SatelliteStreamEvent{
		Type:   eventType,
		Offset: offset,
		Data:   json.RawMessage(payload),
	}, nil
}

func (f *SatelliteStream) setHeaders(request *http.Request) {
	request.Header.Set("Authorization", "Bearer "+f.token)
	request.Header.Set("User-Agent", "aat-aggregator/1.0")
}

func satelliteStatusError(response *http.Response) error {
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&body)
	if body.Error.Code != "" {
		return fmt.Errorf("satellite returned HTTP %d (%s): %s", response.StatusCode, body.Error.Code, body.Error.Message)
	}
	return fmt.Errorf("satellite returned HTTP %d", response.StatusCode)
}
