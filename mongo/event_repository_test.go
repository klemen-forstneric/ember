package mongo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/klemen-forstneric/ember"
)

func env(id, entityID string, ts time.Time) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:        id,
		EntityID:  entityID,
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("correlation_id"): "corr-" + id},
		Timestamp: ts,
	}
}

func ids(envs []ember.EventEnvelope) []string {
	out := make([]string, len(envs))
	for i, e := range envs {
		out[i] = e.ID
	}
	return out
}

type EventRepositorySuite struct {
	suite.Suite
	repo *EventRepository
}

func TestEventRepositorySuite(t *testing.T) {
	suite.Run(t, new(EventRepositorySuite))
}

func (s *EventRepositorySuite) SetupTest() {
	// connectTestMongo (from sort_test.go, same package) skips when mongo is
	// unavailable and drops the per-test collection on cleanup.
	s.repo = NewEventRepository(connectTestMongo(s.T()))
}

func (s *EventRepositorySuite) TestSaveThenListUnpublishedOrdersBySeq() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	// Saved out of order; expect list back in seq (timestamp) order.
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		env("e2", "A", base.Add(2*time.Millisecond)),
		env("e1", "A", base.Add(1*time.Millisecond)),
		env("e3", "B", base.Add(3*time.Millisecond)),
	}))

	got, err := s.repo.ListUnpublished(ctx, 10)
	s.Require().NoError(err)
	s.Equal([]string{"e1", "e2", "e3"}, ids(got))
	// Payload + type + metadata round-trip.
	s.Equal("Created", got[0].Event.Type)
	s.Equal([]byte(`{"k":"v"}`), got[0].Event.Data)
	s.Equal("corr-e1", got[0].Metadata[ember.MetadataKey("correlation_id")])
	// Timestamp reconstructs from created_at (ms precision).
	s.True(got[0].Timestamp.Equal(base.Add(1 * time.Millisecond)))
}

func (s *EventRepositorySuite) TestListUnpublishedRespectsLimit() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		env("e1", "A", base.Add(1)),
		env("e2", "A", base.Add(2)),
		env("e3", "A", base.Add(3)),
	}))

	got, err := s.repo.ListUnpublished(ctx, 2)
	s.Require().NoError(err)
	s.Equal([]string{"e1", "e2"}, ids(got))
}

func (s *EventRepositorySuite) TestMarkPublishedRemovesFromPending() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		env("e1", "A", base.Add(1)),
		env("e2", "A", base.Add(2)),
	}))

	expiresAt := time.Now().UTC().Add(24 * time.Hour)
	s.Require().NoError(s.repo.MarkPublished(ctx, []string{"e1"}, expiresAt))

	got, err := s.repo.ListUnpublished(ctx, 10)
	s.Require().NoError(err)
	s.Equal([]string{"e2"}, ids(got), "published event must drop out of pending")
}

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EventRepository = (*EventRepository)(nil)
