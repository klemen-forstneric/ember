package mongo

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/v2/bson"

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

func versioned(id, entityID string, version uint64, index int, ts time.Time) ember.EventEnvelope {
	e := env(id, entityID, ts)
	e.Version = version
	e.Index = index
	return e
}

func (s *EventRepositorySuite) TestListUnpublishedOrdersByVersionThenIndex() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		versioned("e3", "A", 2, 0, base),
		versioned("e1", "A", 1, 0, base),
		versioned("e2", "A", 1, 1, base),
	}))

	got, err := s.repo.ListUnpublished(ctx, 10, 10)
	s.Require().NoError(err)
	s.Equal([]string{"e1", "e2", "e3"}, ids(got))
	s.Equal(uint64(1), got[0].Version)
	s.Equal(1, got[1].Index)
	s.Equal("Created", got[0].Event.Type)
	s.Equal([]byte(`{"k":"v"}`), got[0].Event.Data)
	s.Equal("corr-e1", got[0].Metadata[ember.MetadataKey("correlation_id")])
}

func (s *EventRepositorySuite) TestListUnpublishedIgnoresTheClock() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		versioned("e1", "A", 1, 0, base.Add(3*time.Second)),
		versioned("e2", "A", 2, 0, base),
		versioned("e3", "A", 3, 0, base),
	}))

	got, err := s.repo.ListUnpublished(ctx, 10, 10)
	s.Require().NoError(err)
	s.Equal([]string{"e1", "e2", "e3"}, ids(got))
}

func (s *EventRepositorySuite) TestListUnpublishedCapsEntitiesAndEventsPerEntity() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	var envs []ember.EventEnvelope
	for _, entity := range []string{"A", "B", "C"} {
		for v := uint64(1); v <= 5; v++ {
			envs = append(envs, versioned(fmt.Sprintf("%s-%d", entity, v), entity, v, 0, base))
		}
	}
	s.Require().NoError(s.repo.Save(ctx, envs))

	got, err := s.repo.ListUnpublished(ctx, 2, 3)
	s.Require().NoError(err)

	perEntity := map[string][]uint64{}
	for _, e := range got {
		perEntity[e.EntityID] = append(perEntity[e.EntityID], e.Version)
	}
	s.Len(perEntity, 2, "at most two entities per round")
	for entity, versions := range perEntity {
		s.LessOrEqual(len(versions), 3, "at most three events for %s", entity)
		s.Equal(uint64(1), versions[0], "each entity's run must start at its lowest unpublished version")
		s.True(slices.IsSorted(versions), "each entity's run must be version-ordered")
	}
}

func (s *EventRepositorySuite) TestListUnpublishedSamplesAcrossEntities() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	var envs []ember.EventEnvelope
	for _, entity := range []string{"A", "B", "C", "D"} {
		envs = append(envs, versioned(entity+"-1", entity, 1, 0, base))
	}
	s.Require().NoError(s.repo.Save(ctx, envs))

	seen := map[string]bool{}
	for range 40 {
		got, err := s.repo.ListUnpublished(ctx, 2, 10)
		s.Require().NoError(err)
		for _, e := range got {
			seen[e.EntityID] = true
		}
	}

	s.Len(seen, 4, "every entity must be reachable across rounds")
}

// TestSaveStoresDataAsADocument pins the reason data is not stored as bytes: an
// operator reading the outbox sees the payload, not base64.
func (s *EventRepositorySuite) TestSaveStoresDataAsADocument() {
	ctx := context.Background()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		env("e1", "A", time.Unix(1_700_000_000, 0).UTC()),
	}))

	var raw bson.M
	s.Require().NoError(s.repo.collection.FindOne(ctx, bson.D{{Key: "_id", Value: "e1"}}).Decode(&raw))
	s.Equal(bson.D{{Key: "k", Value: "v"}}, raw["data"], "want a nested document, not binary")
}

func (s *EventRepositorySuite) TestListUnpublishedRespectsLimit() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		versioned("e1", "A", 1, 0, base),
		versioned("e2", "A", 2, 0, base),
		versioned("e3", "A", 3, 0, base),
	}))

	got, err := s.repo.ListUnpublished(ctx, 10, 2)
	s.Require().NoError(err)
	s.Equal([]string{"e1", "e2"}, ids(got))
}

func (s *EventRepositorySuite) TestMarkPublishedRemovesFromPending() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		versioned("e1", "A", 1, 0, base),
		versioned("e2", "A", 2, 0, base),
	}))

	expiresAt := time.Now().UTC().Add(24 * time.Hour)
	s.Require().NoError(s.repo.MarkPublished(ctx, []string{"e1"}, expiresAt))

	got, err := s.repo.ListUnpublished(ctx, 10, 10)
	s.Require().NoError(err)
	s.Equal([]string{"e2"}, ids(got), "published event must drop out of pending")
}

// Compile-time assertion that the repository satisfies the interfaces.
var (
	_ ember.EventRepository        = (*EventRepository)(nil)
	_ ember.PollingRelayRepository = (*EventRepository)(nil)
)
