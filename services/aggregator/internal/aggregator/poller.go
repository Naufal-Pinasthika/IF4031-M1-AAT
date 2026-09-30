package aggregator

import (
	"context"
	"log/slog"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
)

type Poller struct {
	client *Client
	config config.Config
	logger *slog.Logger
}

type BMKGBatch struct {
	Events        []SeismicEvent
	Warnings      []TsunamiWarning
	CorrelationID string
}

type PVMBGBatch struct {
	Reports       []VolcanicReport
	CorrelationID string
}

func New(client *Client, cfg config.Config, logger *slog.Logger) *Poller {
	return &Poller{client: client, config: cfg, logger: logger}
}

func (p *Poller) PollBMKG(ctx context.Context, since time.Time) (BMKGBatch, error) {
	startedAt := time.Now()
	correlationID := newCorrelationID()
	events, warnings, err := p.client.FetchBMKG(ctx, p.config.BMKGURL, p.config.BMKGAPIKey, correlationID, since)
	if err != nil {
		p.logger.Error("bmkg_poll_failed", "error", err, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
		return BMKGBatch{}, err
	}

	p.logger.Info("bmkg_poll_succeeded", "events", len(events), "warnings", len(warnings), "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
	return BMKGBatch{Events: events, Warnings: warnings, CorrelationID: correlationID}, nil
}

func (p *Poller) PollPVMBG(ctx context.Context, since time.Time) (PVMBGBatch, error) {
	startedAt := time.Now()
	correlationID := newCorrelationID()
	reports, err := p.client.FetchPVMBG(ctx, p.config.PVMBGURL, p.config.PVMBGToken, correlationID, since)
	if err != nil {
		p.logger.Error("pvmbg_poll_failed", "error", err, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
		return PVMBGBatch{}, err
	}

	p.logger.Info("pvmbg_poll_succeeded", "reports", len(reports), "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
	return PVMBGBatch{Reports: reports, CorrelationID: correlationID}, nil
}

func newCorrelationID() string {
	return time.Now().UTC().Format("20060102T150405.000000000Z07:00")
}
