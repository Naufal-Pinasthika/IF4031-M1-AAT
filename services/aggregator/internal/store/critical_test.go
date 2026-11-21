package store

import (
	"slices"
	"testing"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

func TestCriticalProjectionIsAdditionalAndContractDriven(t *testing.T) {
	c := &Coordinator{storage: []config.StorageUnit{{Name: "test"}}, criticalLane: true}
	for _, test := range []struct {
		source   model.HazardSource
		critical any
		want     bool
	}{
		{model.SourceSENSORPANTAI, true, true},
		{model.SourceSENSORPANTAI, false, false},
		{model.SourceSENSORPANTAI, "true", false},
		{model.SourceBMKG, true, false},
	} {
		event := model.HazardEvent{Source: test.source, SourceRefID: "arbitrary-id", Attributes: map[string]any{"critical": test.critical}}
		targets := c.projectionTargets(event)
		if !slices.Contains(targets, "broker") || slices.Contains(targets, "broker:critical") != test.want {
			t.Fatalf("unexpected destinations: %v", targets)
		}
	}
}
