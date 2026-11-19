package store

import "github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"

// p1 conservative status includes a failed replica even with complete fallback.
type ReadStatus struct {
	Partial             bool     `json:"partial"`
	UnavailableUnits    []string `json:"unavailable_units"`
	ReplicationDegraded bool     `json:"replication_degraded,omitempty"`
	LaggingUnits        []string `json:"lagging_units,omitempty"`
	coverageIncomplete  bool     // internal bound proof; triggers a bounded refill
}

// p1
type EventPage struct {
	Events     []model.HazardEvent `json:"events"`
	NextCursor *string             `json:"next_cursor"`
	ReadStatus
}

// p1
type EventLookup struct {
	Event *model.HazardEvent `json:"event"`
	ReadStatus
}
