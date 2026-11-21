package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestM2IngressUsesSignedIdentityNotCallerHeaders(t *testing.T) {
	count := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		if r.Header.Get("X-Client-Role") != "ops" || r.Header.Get("X-Client-ID") != "ops-one" {
			t.Error("caller identity forwarded", r.Header)
		}
		w.WriteHeader(202)
	}))
	defer upstream.Close()
	s := &Server{client: upstream.Client(), aggregatorURL: upstream.URL, aggregatorKey: "private", fieldClientID: "field-one"}
	for _, claims := range []*AccessClaims{{ClientID: "media", Scope: scopeSummary}, {ClientID: "field-one", Scope: scopeRaw}} {
		r := httptest.NewRequest("POST", "/unprocessed/replay", nil)
		r.Header.Set("X-Client-Role", "ops")
		r = r.WithContext(context.WithValue(r.Context(), claimsContextKey, claims))
		w := httptest.NewRecorder()
		s.forwardIngress(w, r, "/internal/unprocessed/replay", true)
		if w.Code != 403 {
			t.Fatal(w.Code)
		}
	}
	r := httptest.NewRequest("POST", "/unprocessed/replay", nil)
	r.Header.Set("X-Client-Role", "field")
	r.Header.Set("X-Client-ID", "attacker")
	r = r.WithContext(context.WithValue(r.Context(), claimsContextKey, &AccessClaims{ClientID: "ops-one", Scope: scopeOps}))
	w := httptest.NewRecorder()
	s.forwardIngress(w, r, "/internal/unprocessed/replay", true)
	if count != 1 || w.Code != 202 {
		t.Fatal(count, w.Code)
	}
}
