package consumer

import (
	"context"
	"errors"
	"sync"
)

// Each lane owns its Kafka reader, offset commits and bounded prefetch queue.
// One failing/slow reader cannot stop another lane from fetching messages.
// The shared durable store deduplicates notifications across both topics.
type Lanes []*Consumer

func (lanes Lanes) Run(ctx context.Context) {
	var workers sync.WaitGroup
	for _, lane := range lanes {
		workers.Add(1)
		go func(c *Consumer) {
			defer workers.Done()
			c.Run(ctx)
		}(lane)
	}
	workers.Wait()
}

func (lanes Lanes) Probe(ctx context.Context) error {
	for _, lane := range lanes {
		if err := lane.Probe(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (lanes Lanes) Close() error {
	var result error
	for _, lane := range lanes {
		result = errors.Join(result, lane.Close())
	}
	return result
}
