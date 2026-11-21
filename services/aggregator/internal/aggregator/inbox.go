package aggregator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

// JSON validation precedes decoding: encoding/json otherwise replaces invalid
// UTF-8, accepts absent struct fields and can allocate a deeply nested object.
func ValidateJSON(payload []byte) error {
	if len(payload) > store.MaxIngressBytes {
		return fmt.Errorf("payload_too_large")
	}
	if !utf8.Valid(payload) {
		return fmt.Errorf("invalid_utf8")
	}
	depth := 0
	quoted, escaped := false, false
	for _, b := range payload {
		if quoted {
			if escaped {
				escaped = false
			} else if b == '\\' {
				escaped = true
			} else if b == '"' {
				quoted = false
			}
			continue
		}
		if b == '"' {
			quoted = true
		} else if b == '{' || b == '[' {
			depth++
			if depth > 64 {
				return fmt.Errorf("nesting_too_deep")
			}
		} else if b == '}' || b == ']' {
			depth--
		}
	}
	if !json.Valid(payload) {
		return fmt.Errorf("invalid_json")
	}
	return nil
}

func DecodeVolcanic(payload []byte, version int) (model.HazardEvent, error) {
	if err := ValidateJSON(payload); err != nil {
		return model.HazardEvent{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return model.HazardEvent{}, err
	}
	if version >= 2 {
		if _, ok := fields["report_id"]; !ok {
			fields["report_id"] = fields["report_ref"]
		}
		if raw := fields["eruption_count_24h"]; len(raw) > 0 && raw[0] == '"' {
			var text string
			if err := json.Unmarshal(raw, &text); err != nil {
				return model.HazardEvent{}, err
			}
			n, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return model.HazardEvent{}, fmt.Errorf("schema_incompatible: eruption_count_24h")
			}
			fields["eruption_count_24h"] = json.RawMessage(strconv.FormatInt(n, 10))
		}
	}
	for _, key := range []string{"report_id", "volcano_id", "alert_level", "eruption_count_24h", "ash_column_height_m", "reported_at"} {
		if len(fields[key]) == 0 || bytes.Equal(fields[key], []byte("null")) {
			return model.HazardEvent{}, fmt.Errorf("schema_incompatible: missing %s", key)
		}
	}
	normalized, _ := json.Marshal(fields)
	if raw, ok := fields["confidence_level"]; ok {
		var confidence float64
		if json.Unmarshal(raw, &confidence) != nil || bytes.Equal(raw, []byte("null")) || confidence < 0 || confidence > 1 {
			return model.HazardEvent{}, fmt.Errorf("contract_violation: confidence_level")
		}
	}
	var report VolcanicReport
	if err := json.Unmarshal(normalized, &report); err != nil {
		return model.HazardEvent{}, fmt.Errorf("schema_incompatible: %w", err)
	}
	if report.ReportID == "" || report.ReportedAt.IsZero() || report.EruptionCount24H < 0 || report.AshColumnHeightM < 0 {
		return model.HazardEvent{}, fmt.Errorf("contract_violation: invalid volcanic report")
	}
	switch report.AlertLevel {
	case AlertNormal, AlertWaspada, AlertSiaga, AlertAwas:
	default:
		return model.HazardEvent{}, fmt.Errorf("contract_violation: alert_level")
	}
	return mapVolcanic(report)
}

// A temporary spool bounds memory even for an oversized body. It is removed
// only after the full byte stream has been committed to durable chunk storage.
func (a *Aggregator) pollPVMBGInbox(ctx context.Context, token int64) error {
	c, ok := a.coordinator.(*store.Coordinator)
	if !ok {
		return fmt.Errorf("durable coordinator unavailable")
	}
	since, err := c.TimeCursor(ctx, "PVMBG", a.config.StartSince)
	if err != nil {
		return err
	}
	endpoint, err := url.Parse(a.config.PVMBGURL + "/volcanic-reports")
	if err != nil {
		return err
	}
	q := endpoint.Query()
	q.Set("since", since.UTC().Format(time.RFC3339))
	endpoint.RawQuery = q.Encode()
	request, err := http.NewRequestWithContext(ctx, "GET", endpoint.String(), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+a.config.PVMBGToken)
	request.Header.Set("X-Correlation-ID", correlation.ID(ctx))
	started := time.Now()
	response, err := a.poller.client.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("PVMBG returned HTTP %d", response.StatusCode)
	}
	spool, err := os.CreateTemp("", "aat-pvmbg-*")
	if err != nil {
		return err
	}
	defer os.Remove(spool.Name())
	defer spool.Close()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(spool, hash), response.Body)
	if err != nil {
		return err
	}
	if _, err = spool.Seek(0, 0); err != nil {
		return err
	}
	fingerprint := hex.EncodeToString(hash.Sum(nil))
	pageID := "msg-pvmbg-" + fingerprint
	// Unparseable pages are parked intact; good pages are split into independent
	// identities so a poison item cannot stop unrelated valid reports.
	var payload []byte
	if size <= store.MaxIngressBytes {
		payload, err = io.ReadAll(spool)
		if err != nil {
			return err
		}
	}
	reason := ValidateJSON(payload)
	if size > store.MaxIngressBytes {
		reason = fmt.Errorf("payload_too_large: %d bytes", size)
	}
	var reports []json.RawMessage
	if reason == nil {
		reason = json.Unmarshal(payload, &reports)
		if reports == nil && reason == nil {
			reason = fmt.Errorf("contract_violation: expected array")
		}
	}
	if reason != nil {
		_, _ = spool.Seek(0, 0)
		if _, err = c.StageInbox(ctx, pageID, "PVMBG", pageID, "invalid-page", fingerprint, spool, store.InboxLease{Holder: a.config.InstanceID, Token: token, CorrelationID: correlation.ID(ctx)}); err != nil {
			return err
		}
		return nil // asynchronous validation parks it; the source remains observable
	}
	var newest time.Time
	for _, raw := range reports {
		var identity struct {
			ID  string    `json:"report_id"`
			Ref string    `json:"report_ref"`
			At  time.Time `json:"reported_at"`
		}
		_ = json.Unmarshal(raw, &identity)
		id := store.InboxID("PVMBG", raw)
		ref := identity.ID
		if ref == "" {
			ref = identity.Ref
		}
		if ref == "" {
			ref = id
		}
		if _, err = c.StageInbox(ctx, id, "PVMBG", ref, "volcanic-report", id, bytes.NewReader(raw), store.InboxLease{Holder: a.config.InstanceID, Token: token, CorrelationID: correlation.ID(ctx)}); err != nil {
			return err
		}
		if identity.At.After(newest) {
			newest = identity.At
		}
	}
	if !newest.IsZero() {
		if err = c.AdvanceTimeCursor(ctx, "PVMBG", a.config.InstanceID, token, newest); err != nil {
			return err
		}
	}
	a.logger.Info("pvmbg_inbox_staged", "records", len(reports), "latency_ms", time.Since(started).Milliseconds(), "correlation_id", request.Header.Get("X-Correlation-ID"))
	return nil
}

func (a *Aggregator) RunInbox(ctx context.Context) {
	c, ok := a.coordinator.(*store.Coordinator)
	if !ok {
		return
	}
	for ctx.Err() == nil {
		worked := false
		for _, source := range []string{"FIELD_REPORT", "PVMBG"} {
			done, err := c.ProcessInbox(ctx, source, func(m store.InboxMessage) (*model.HazardEvent, error) {
				if err := ValidateJSON(m.Payload); err != nil {
					return nil, err
				}
				if source == "FIELD_REPORT" {
					return nil, nil
				}
				if m.Kind != "volcanic-report" {
					return nil, fmt.Errorf("contract_violation: invalid source page")
				}
				event, err := DecodeVolcanic(m.Payload, a.config.PVMBGAdapterVersion)
				return &event, err
			})
			if err != nil {
				a.logger.Warn("inbox_processing_retry", "source", source, "error", err)
			}
			worked = worked || done
		}
		if !worked {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func ValidReportID(id string) bool {
	return len(id) > 0 && len(id) <= 128 && !strings.ContainsAny(id, "\x00\r\n")
}
