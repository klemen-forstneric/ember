package ember

import "context"

// saveOnlyRepository has no drain methods, like a WAL-backed store whose drain
// is a replication slot.
type saveOnlyRepository struct{}

func (saveOnlyRepository) Save(context.Context, []EventEnvelope) error { return nil }

var _ EventRepository = saveOnlyRepository{}
