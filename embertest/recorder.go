package embertest

import (
	"context"
	"fmt"
	"sync"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/json"
)

var _ ember.Sink = (*Recorder)(nil)

// Recorder captures what a Save delivered and decodes it back into events.
type Recorder struct {
	marshaler ember.EventMarshaler
	publisher *ember.Publisher

	mu        sync.Mutex
	envelopes []ember.EventEnvelope
}

// NewRecorder takes pointer literals: json.NewEventMarshaler reflects with
// .Elem() and panics on a value struct. Registration affects decoding only, so
// an unregistered event still reaches the sink and fails in Events(). Construct
// one per test — it accumulates.
func NewRecorder(events ...ember.Event) *Recorder {
	m := json.NewEventMarshaler(events...)
	r := &Recorder{marshaler: m}
	r.publisher = ember.NewPublisher(&ider{}, ember.NopMetadataGetter{}, m, ember.BestEffort(r))
	return r
}

func (r *Recorder) Publisher() *ember.Publisher { return r.publisher }

func (r *Recorder) Publish(_ context.Context, envelopes []ember.EventEnvelope) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.envelopes = append(r.envelopes, envelopes...)
	return nil
}

// Events returns the captured events in publish order.
func (r *Recorder) Events() []ember.Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]ember.Event, 0, len(r.envelopes))
	for i := range r.envelopes {
		e, err := r.marshaler.Unmarshal(context.Background(), r.envelopes[i].Event)
		if err != nil {
			panic(err) // an unregistered event type: pass it to NewRecorder
		}
		out = append(out, e)
	}
	return out
}

func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.envelopes = nil
}

// ider mints deterministic event ids so recorded envelopes stay comparable.
type ider struct {
	mu sync.Mutex
	n  int
}

func (i *ider) ID() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.n++
	return fmt.Sprintf("evt-%d", i.n)
}
