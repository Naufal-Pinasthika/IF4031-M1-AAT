package store

import "context"

type readPriorityKey struct{}

func WithReadPriority(ctx context.Context, priority bool) context.Context {
	return context.WithValue(ctx, readPriorityKey{}, priority)
}

func ReadPriority(ctx context.Context) bool {
	priority, _ := ctx.Value(readPriorityKey{}).(bool)
	return priority
}
