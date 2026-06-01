package ember

import "context"

// MetadataKey
type MetadataKey string

// Metadata
type Metadata map[MetadataKey]interface{}

// MetadataGetter
type MetadataGetter interface {
	Get(ctx context.Context) (Metadata, error)
}

// NoopMetadataGetter
type NoopMetadataGetter struct {
}

func (NoopMetadataGetter) Get(ctx context.Context) (Metadata, error) {
	return make(Metadata), nil
}
