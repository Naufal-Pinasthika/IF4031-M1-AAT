package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestForwardedPriorityComesFromSignedClaimsNotHeader(t *testing.T) {
	for _, id := range []string{"field-client", "media-client"} {
		t.Run(id, func(t *testing.T) {
			var priority string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				priority = r.Header.Get("X-Request-Priority")
				_, _ = w.Write([]byte("{}"))
			}))
			defer upstream.Close()
			s := &Server{client: upstream.Client(), aggregatorURL: upstream.URL, aggregatorKey: "test", fieldClientID: "field-client"}
			r := httptest.NewRequest("GET", "/events", nil)
			r.Header.Set("X-Request-Priority", "field") // Attacker-controlled, including Media.
			r = r.WithContext(context.WithValue(r.Context(), claimsContextKey, &AccessClaims{ClientID: id}))
			response, _, err := s.callAggregator(r, "/internal/events")
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if (priority == "field") != (id == "field-client") {
				t.Fatalf("wrong forwarded priority %q for %s", priority, id)
			}
		})
	}
}
