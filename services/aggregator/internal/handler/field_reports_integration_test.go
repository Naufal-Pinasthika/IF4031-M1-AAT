package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestM2FieldReportHTTPDurabilityIdempotencyAndOpsBoundary(t *testing.T) {
	raw := os.Getenv("P2_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated database required")
	}
	ctx := t.Context()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p3_handler_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	u, _ := url.Parse(raw)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	coordinator, err := store.NewCoordinator(ctx, u.String(), 2*time.Second, logger)
	if err != nil {
		t.Fatal(err)
	}
	h := NewP1(nil, coordinator, "")
	body := []byte(`{"report_id":"offsite-1","description":"Flood observed"}`)
	post := func(data []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/internal/field-reports", bytes.NewReader(data))
		r.Header.Set("X-Client-Role", "field")
		r.Header.Set("X-Client-ID", "field-one")
		r.Header.Set("Idempotency-Key", "offsite-1")
		w := httptest.NewRecorder()
		h.PostFieldReport(w, r)
		return w
	}
	started := time.Now()
	response := post(body)
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body.String())
	}
	if time.Since(started) > time.Second {
		t.Fatal("202 waited for downstream")
	}
	var ack struct {
		ReportID string `json:"report_id"`
	}
	_ = json.Unmarshal(response.Body.Bytes(), &ack)
	// No processor is running. The receiver/database client is discarded after
	// the HTTP acknowledgement; another process must recover the same inbox.
	coordinator.Close()
	coordinator, err = store.NewCoordinator(ctx, u.String(), 2*time.Second, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	h = NewP1(nil, coordinator, "")
	for range 5 {
		if w := post(body); w.Code != 202 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if w := post([]byte(`{"report_id":"offsite-1","description":"different"}`)); w.Code != 409 {
		t.Fatal("different payload reused idempotency key", w.Code)
	}
	if _, err = coordinator.FieldReport(ctx, ack.ReportID, "field-one", false); err != nil {
		t.Fatal("pending report inaccessible", err)
	}
	for {
		worked, err := coordinator.ProcessInbox(ctx, "FIELD_REPORT", func(store.InboxMessage) (*model.HazardEvent, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	var count int
	if err = admin.QueryRow(ctx, "SELECT count(*) FROM "+schema+".field_report_results").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	if _, err = coordinator.FieldReport(ctx, ack.ReportID, "another-field", false); err == nil {
		t.Fatal("cross-client disclosure")
	}
	for _, role := range []string{"field", "media", ""} {
		r := httptest.NewRequest("GET", "/unprocessed/x", nil)
		r.Header.Set("X-Client-Role", role)
		w := httptest.NewRecorder()
		OperationsOnly(h.GetInboxDetails)(w, r)
		if w.Code != 403 {
			t.Fatal(role, w.Code)
		}
	}
	if w := post([]byte(strings.Repeat("x", 70<<10))); w.Code != 413 {
		t.Fatal(w.Code)
	}
}
