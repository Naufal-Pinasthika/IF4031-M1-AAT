package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/service"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/store"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/token"
)

type Handler struct {
	service *service.Service
	signer  *token.Signer
	store   *store.Store
	logger  *slog.Logger
}

func New(service *service.Service, signer *token.Signer, store *store.Store, logger *slog.Logger) *Handler {
	return &Handler{service: service, signer: signer, store: store, logger: logger}
}

func (h *Handler) Token(w http.ResponseWriter, r *http.Request) {
	client, ok := h.authenticateClient(w, r)
	if !ok {
		return
	}
	pair, err := h.service.Issue(r.Context(), client)
	if err != nil {
		h.logger.Error("token_issue_failed", "client_id", client.ID, "error", err, "correlation_id", CorrelationID(r.Context()))
		writeError(w, http.StatusInternalServerError, "server_error", "failed to issue token")
		return
	}
	h.logger.Info("token_issued", "client_id", client.ID, "correlation_id", CorrelationID(r.Context()))
	writeJSON(w, http.StatusOK, pair)
}

func (h *Handler) Refresh(w http.ResponseWriter, r *http.Request) {
	client, ok := h.authenticateClient(w, r)
	if !ok {
		return
	}
	var request struct {
		RefreshToken string `json:"refresh_token"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || strings.TrimSpace(request.RefreshToken) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "refresh_token is required")
		return
	}
	pair, err := h.service.Refresh(r.Context(), client, request.RefreshToken)
	if err != nil {
		if errors.Is(err, store.ErrInvalidRefreshToken) || errors.Is(err, store.ErrRefreshReuse) {
			h.logger.Warn("refresh_rejected", "client_id", client.ID, "reason", err.Error(), "correlation_id", CorrelationID(r.Context()))
			writeError(w, http.StatusUnauthorized, "invalid_refresh_token", "refresh token is invalid, expired, or already used")
			return
		}
		h.logger.Error("refresh_failed", "client_id", client.ID, "error", err, "correlation_id", CorrelationID(r.Context()))
		writeError(w, http.StatusInternalServerError, "server_error", "failed to refresh token")
		return
	}
	h.logger.Info("token_refreshed", "client_id", client.ID, "correlation_id", CorrelationID(r.Context()))
	writeJSON(w, http.StatusOK, pair)
}

func (h *Handler) JWKS(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "public, max-age=30")
	writeJSON(w, http.StatusOK, h.signer.JWKS())
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if err := h.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "unhealthy", "auth database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) authenticateClient(w http.ResponseWriter, r *http.Request) (store.Client, bool) {
	clientID, secret, ok := r.BasicAuth()
	if !ok || clientID == "" || secret == "" {
		w.Header().Set("WWW-Authenticate", `Basic realm="bnpb-auth"`)
		writeError(w, http.StatusUnauthorized, "invalid_client", "client authentication is required")
		return store.Client{}, false
	}
	client, err := h.service.Authenticate(r.Context(), clientID, secret)
	if err != nil {
		if errors.Is(err, store.ErrInvalidClient) {
			h.logger.Warn("client_authentication_failed", "client_id", clientID, "correlation_id", CorrelationID(r.Context()))
			w.Header().Set("WWW-Authenticate", `Basic realm="bnpb-auth"`)
			writeError(w, http.StatusUnauthorized, "invalid_client", "invalid client credentials")
			return store.Client{}, false
		}
		h.logger.Error("client_authentication_error", "client_id", clientID, "error", err, "correlation_id", CorrelationID(r.Context()))
		writeError(w, http.StatusInternalServerError, "server_error", "client authentication failed")
		return store.Client{}, false
	}
	return client, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, description string) {
	writeJSON(w, status, map[string]string{"error": code, "error_description": description})
}
