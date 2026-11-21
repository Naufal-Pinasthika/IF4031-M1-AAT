package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/runtimeprofile"
)

func startProfiling(ctx context.Context, logger *slog.Logger) error {
	return runtimeprofile.Start(ctx, logger, os.Getenv("AGGREGATOR_PROFILE_ENABLED") == "1", buildVersion)
}
func profilingHandler() http.Handler { return runtimeprofile.Handler(buildVersion) }
