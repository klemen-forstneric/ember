package ember

import "context"

type Notifier interface {
	Notify(ctx context.Context, envelopes []EventEnvelope)
}
