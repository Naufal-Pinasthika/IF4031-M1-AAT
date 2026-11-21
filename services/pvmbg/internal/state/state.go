package state

import "sync"

// schemaV1: json schema without "confidence_level"
// schemaV2: json schema with "confidence_level"

type ServiceState struct {
	mu              sync.RWMutex // use mutex to prevent data race changing schema version / outtage
	schemaV2Enabled bool
	outageEnabled   bool
	schemaMode      string
}

func (s *ServiceState) SetSchemaMode(mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemaMode = mode
	s.schemaV2Enabled = mode == "additive"
}
func (s *ServiceState) SchemaMode() string { s.mu.RLock(); defer s.mu.RUnlock(); return s.schemaMode }

// constructor
func NewServiceState() *ServiceState {
	return &ServiceState{}
}

// func to check schema
func (s *ServiceState) IsSchemaV2() bool {
	// read lock
	s.mu.RLock()

	// after func finished, read unlock
	defer s.mu.RUnlock()
	return s.schemaV2Enabled
}

// func to check outage
func (s *ServiceState) IsOutage() bool {
	// read lock
	s.mu.RLock()

	// after func finished, read unlock
	defer s.mu.RUnlock()
	return s.outageEnabled
}

func (s *ServiceState) ToggleSchemaVersion() bool {
	// lock
	s.mu.Lock()

	// after func finished, unlock
	defer s.mu.Unlock()

	// return exact opposite state (hence 'toggle')
	s.schemaV2Enabled = !s.schemaV2Enabled
	s.schemaMode = "legacy"
	if s.schemaV2Enabled {
		s.schemaMode = "additive"
	}
	return s.schemaV2Enabled
}

func (s *ServiceState) ToggleOutage() bool {
	// lock
	s.mu.Lock()

	// after func finished, unlock
	defer s.mu.Unlock()

	// return exact opposite state (hence 'toggle')
	s.outageEnabled = !s.outageEnabled
	return s.outageEnabled
}
