package aggregator

import (
	"context"
	"log/slog"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
)

type Service struct {
	client Client
	config config.Config
	logger *slog.Logger
}

func New(client Client, cfg config.Config, logger *slog.Logger) *Service {
	return &Service{client: client, config: cfg, logger: logger}
}

func (s *Service) Run(ctx context.Context) {
	go s.runBMKG(ctx)
	go s.runPVMBG(ctx)
	<-ctx.Done()
}

func (s *Service) runBMKG(ctx context.Context) {
	since := s.config.StartSince
	ticker := time.NewTicker(s.config.PollingInterval)
	defer ticker.Stop()

	for {
		s.pollBMKG(ctx, &since)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) runPVMBG(ctx context.Context) {
	since := s.config.StartSince
	ticker := time.NewTicker(s.config.PollingInterval)
	defer ticker.Stop()

	for {
		s.pollPVMBG(ctx, &since)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) pollBMKG(ctx context.Context, since *time.Time) {
	startedAt := time.Now()
	correlationID := newCorrelationID()
	events, warnings, err := s.client.FetchBMKG(ctx, s.config.BMKGURL, s.config.BMKGAPIKey, correlationID, *since)
	if err != nil {
		s.logger.Error("bmkg_poll_failed", "error", err, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
		return
	}

	for _, event := range events {
		if event.OccurredAt.After(*since) {
			*since = event.OccurredAt
		}
	}
	s.logger.Info("bmkg_poll_succeeded", "events", len(events), "warnings", len(warnings), "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
}

func (s *Service) pollPVMBG(ctx context.Context, since *time.Time) {
	startedAt := time.Now()
	correlationID := newCorrelationID()
	reports, err := s.client.FetchPVMBG(ctx, s.config.PVMBGURL, s.config.PVMBGToken, correlationID, *since)
	if err != nil {
		s.logger.Error("pvmbg_poll_failed", "error", err, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
		return
	}

	for _, report := range reports {
		if report.ReportedAt.After(*since) {
			*since = report.ReportedAt
		}
	}
	s.logger.Info("pvmbg_poll_succeeded", "reports", len(reports), "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
}

func newCorrelationID() string {
	return time.Now().UTC().Format("20060102T150405.000000000Z07:00")
}
