package mongo

import (
	"context"
	"fmt"
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

func (s *EventRepositorySuite) TestListUnpublishedRejectsNonPositiveLimit() {
	for _, limit := range []int{0, -1} {
		_, err := s.repo.ListUnpublished(context.Background(), 0, limit)
		s.Require().ErrorIs(err, ember.ErrInvalidLimit)
	}
}

func (s *EventRepositorySuite) TestListUnpublishedOrdersByVersionThenIndex() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		versioned("e3", "A", 2, 0, base),
		versioned("e1", "A", 1, 0, base),
		versioned("e2", "A", 1, 1, base),
	}))

	got, err := s.repo.ListUnpublished(ctx, 0, 10)
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
		versioned("e2", "A", 2, 0, base),
		versioned("e3", "A", 3, 0, base),
		versioned("e1", "A", 1, 0, base.Add(3*time.Second)),
	}))

	got, err := s.repo.ListUnpublished(ctx, 0, 10)
	s.Require().NoError(err)
	s.Equal([]string{"e1", "e2", "e3"}, ids(got))
}

func (s *EventRepositorySuite) TestListUnpublishedOrdersAcrossEntitiesByEntityIDThenVersion() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		versioned("c-2", "C", 2, 0, base),
		versioned("a-2", "A", 2, 0, base),
		versioned("c-1", "C", 1, 0, base),
		versioned("b-1", "B", 1, 0, base),
		versioned("a-1", "A", 1, 0, base),
	}))

	got, err := s.repo.ListUnpublished(ctx, 0, 10)
	s.Require().NoError(err)
	s.Equal([]string{"a-1", "a-2", "b-1", "c-1", "c-2"}, ids(got),
		"flat order must group runs by entity_id, each run a version-ordered prefix")
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

	got, err := s.repo.ListUnpublished(ctx, 0, 2)
	s.Require().NoError(err)
	s.Equal([]string{"e1", "e2"}, ids(got))
}

func (s *EventRepositorySuite) TestPartitionsSplitPendingRowsByEntity() {
	ctx := context.Background()
	repo := NewEventRepository(s.repo.collection, WithPartitions(4))
	base := time.Unix(1_700_000_000, 0).UTC()
	var envs []ember.EventEnvelope
	for i := range 20 {
		envs = append(envs, versioned(fmt.Sprintf("e%d", i), fmt.Sprintf("entity-%d", i), 1, 0, base))
	}
	s.Require().NoError(repo.Save(ctx, envs))

	seen := map[string]int{}
	for p := range repo.Partitions() {
		got, err := repo.ListUnpublished(ctx, p, 100)
		s.Require().NoError(err)
		for _, e := range got {
			s.Equal(p, ember.Partition(e.EntityID, 4), "%s listed in the wrong partition", e.EntityID)
			seen[e.ID]++
		}
	}
	s.Len(seen, 20, "every row is in exactly one partition")
	for id, n := range seen {
		s.Equal(1, n, id)
	}
}

func (s *EventRepositorySuite) TestRowsWithoutPartitionBelongToPartitionZero() {
	ctx := context.Background()
	repo := NewEventRepository(s.repo.collection, WithPartitions(4))
	// A row written before partitioning: no partition field at all.
	_, err := s.repo.collection.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "legacy"}, {Key: "entity_id", Value: "A"}, {Key: "type", Value: "Created"},
		{Key: "data", Value: bson.D{}}, {Key: "version", Value: 1}, {Key: "idx", Value: 0},
		{Key: "created_at", Value: time.Unix(1_700_000_000, 0).UTC()}, {Key: "published", Value: false},
	})
	s.Require().NoError(err)

	got, err := repo.ListUnpublished(ctx, 0, 10)
	s.Require().NoError(err)
	s.Equal([]string{"legacy"}, ids(got))
	for p := 1; p < 4; p++ {
		got, err := repo.ListUnpublished(ctx, p, 10)
		s.Require().NoError(err)
		s.Empty(got)
	}
}

func (s *EventRepositorySuite) TestListUnpublishedKeepsTheOrderingKey() {
	ctx := context.Background()
	e := env("e1", "message-1", time.Unix(1_700_000_000, 0).UTC())
	e.OrderingKey = "chat-1"
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{e}))

	got, err := s.repo.ListUnpublished(ctx, 0, 10)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("chat-1", got[0].OrderingKey)
	s.Equal("message-1", got[0].EntityID)
}

func (s *EventRepositorySuite) TestRowsWithoutOrderingKeyUseTheEntityID() {
	ctx := context.Background()
	// A row written before ordering keys: no ordering_key field at all.
	_, err := s.repo.collection.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "legacy"}, {Key: "entity_id", Value: "A"}, {Key: "type", Value: "Created"},
		{Key: "data", Value: bson.D{}}, {Key: "version", Value: 1}, {Key: "idx", Value: 0},
		{Key: "partition", Value: 0}, {Key: "created_at", Value: time.Unix(1_700_000_000, 0).UTC()},
		{Key: "published", Value: false},
	})
	s.Require().NoError(err)

	got, err := s.repo.ListUnpublished(ctx, 0, 10)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("A", got[0].OrderingKey)
}

func (s *EventRepositorySuite) TestOnePartitionByDefault() {
	s.Equal(1, s.repo.Partitions())
	s.Equal(1, NewEventRepository(s.repo.collection, WithPartitions(0)).Partitions())
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

	got, err := s.repo.ListUnpublished(ctx, 0, 10)
	s.Require().NoError(err)
	s.Equal([]string{"e2"}, ids(got), "published event must drop out of pending")
}

// Compile-time assertion that the repository satisfies the interfaces.
var (
	_ ember.EventRepository        = (*EventRepository)(nil)
	_ ember.PollingRelayRepository = (*EventRepository)(nil)
)
