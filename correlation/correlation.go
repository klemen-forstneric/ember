package correlation

import (
	"context"
	"errors"
)

type contextKey string

const (
	ContextKey contextKey = "correlation_id"
)

var (
	ErrNotFound = errors.New("correlation id not found in context")
)

func NewContext(parent context.Context, correlationID string) context.Context {
	return context.WithValue(parent, ContextKey, correlationID)
}

func FromContext(ctx context.Context) (string, error) {
	correlationID, ok := ctx.Value(ContextKey).(string)
	if !ok {
		return "", ErrNotFound
	}

	return correlationID, nil
}
