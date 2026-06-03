package ext

import (
	"context"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/correlation"
)

const (
	MetadataKeyCorrelationID ember.MetadataKey = "correlation_id"
)

type MetadataGetter struct{}

func (m *MetadataGetter) Get(ctx context.Context) (ember.Metadata, error) {
	correlationId, err := correlation.FromContext(ctx)
	if err != nil {
		return nil, err
	}

	return ember.Metadata{
		MetadataKeyCorrelationID: correlationId,
	}, nil
}
