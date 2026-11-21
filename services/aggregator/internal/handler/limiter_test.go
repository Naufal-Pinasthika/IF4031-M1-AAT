package handler

import (
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAggregatorAdmissionPreservesFieldReserve(t *testing.T) {
	l := NewPriorityLimiter(4, 2)
	if !l.acquire(false) || !l.acquire(false) || l.acquire(false) {
		t.Fatal("regular admission escaped shared budget")
	}
	called := false
	h := l.Wrap(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if !store.ReadPriority(r.Context()) {
			t.Fatal("priority context not propagated")
		}
	})
	r := httptest.NewRequest("GET", "/internal/events", nil)
	r.Header.Set("X-Request-Priority", "field")
	w := httptest.NewRecorder()
	h(w, r)
	if !called || w.Code != 200 {
		t.Fatal("field blocked by regular saturation")
	}
	if l.total != 2 || l.regular != 2 {
		t.Fatal("field release corrupted regular budget")
	}
}
