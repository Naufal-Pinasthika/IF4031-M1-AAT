package correlation

import "context"

type idKey struct{}

func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, idKey{}, id)
}

func ID(ctx context.Context) string {
	id, _ := ctx.Value(idKey{}).(string)
	return id
}
