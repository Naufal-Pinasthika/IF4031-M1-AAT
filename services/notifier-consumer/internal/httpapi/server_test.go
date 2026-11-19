package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/store"
)

func TestNotificationsValidatePagination(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "http.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := New(s, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second)
	for _, path := range []string{"/notifications?limit=0", "/notifications?limit=1001", "/notifications?limit=no", "/notifications?cursor=!"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != 400 {
			t.Fatalf("%s: %d", path, response.Code)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/notifications", nil))
	var page store.NotificationPage
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &page) != nil || page.Notifications == nil || len(page.Notifications) != 0 || page.NextCursor != "" {
		t.Fatalf("unexpected empty page: %d %s", response.Code, response.Body.String())
	}
}
