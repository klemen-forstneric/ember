package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/klemen-forstneric/ember"
)

// marshaled builds a save at the version that replaces stored version v.
func marshaled(typ, id string, v uint64, data string) *ember.MarshaledEntity {
	return &ember.MarshaledEntity{
		ID:      id,
		Type:    typ,
		Version: ember.NewVersion(v).Inc(),
		Data:    []byte(data),
	}
}

type EntityRepositorySuite struct {
	suite.Suite
	repo *EntityRepository
}

func TestEntityRepositorySuite(t *testing.T) {
	suite.Run(t, new(EntityRepositorySuite))
}

func (s *EntityRepositorySuite) SetupTest() {
	// connectTestMongo (from sort_test.go, same package) skips when mongo is
	// unavailable and drops the per-test collection on cleanup.
	col := connectTestMongo(s.T())

	// No explicit EnsureEntities: the constructor must provision the index,
	// which is what TestStaleVersionConflicts then depends on.
	repo, err := NewEntityRepository(context.Background(), col)
	s.Require().NoError(err)
	s.repo = repo
}

func (s *EntityRepositorySuite) TestSaveThenGetRoundTrips() {
	ctx := context.Background()
	s.Require().NoError(s.repo.Save(ctx, marshaled("order", "1", 0, `{"n":"a"}`)))

	got, err := s.repo.Get(ctx, "order", "1")
	s.Require().NoError(err)
	s.Equal("1", got.ID)
	s.Equal("order", got.Type)
	s.Equal(uint64(1), got.Version.Value())
	s.JSONEq(`{"n":"a"}`, string(got.Data))
}

func (s *EntityRepositorySuite) TestGetMissingIsNotFound() {
	_, err := s.repo.Get(context.Background(), "order", "nope")
	s.ErrorIs(err, ember.ErrEntityNotFound)
}

// Identity is (type, id), so two types may share an id in one collection.
func (s *EntityRepositorySuite) TestSameIDAcrossTypesCoexist() {
	ctx := context.Background()
	s.Require().NoError(s.repo.Save(ctx, marshaled("order", "shared", 0, `{"n":"order"}`)))
	s.Require().NoError(s.repo.Save(ctx, marshaled("offer", "shared", 0, `{"n":"offer"}`)))

	order, err := s.repo.Get(ctx, "order", "shared")
	s.Require().NoError(err)
	s.JSONEq(`{"n":"order"}`, string(order.Data))

	offer, err := s.repo.Get(ctx, "offer", "shared")
	s.Require().NoError(err)
	s.JSONEq(`{"n":"offer"}`, string(offer.Data))

	s.Require().NoError(s.repo.Save(ctx, marshaled("order", "shared", 1, `{"n":"order2"}`)))

	offer, err = s.repo.Get(ctx, "offer", "shared")
	s.Require().NoError(err)
	s.Equal(uint64(1), offer.Version.Value(), "the sibling type must be untouched")
	s.JSONEq(`{"n":"offer"}`, string(offer.Data))
}

func (s *EntityRepositorySuite) TestStaleVersionConflicts() {
	ctx := context.Background()
	s.Require().NoError(s.repo.Save(ctx, marshaled("order", "1", 0, `{"n":"a"}`)))
	s.Require().NoError(s.repo.Save(ctx, marshaled("order", "1", 1, `{"n":"b"}`)))

	s.ErrorIs(s.repo.Save(ctx, marshaled("order", "1", 1, `{"n":"c"}`)), ember.ErrVersionConflict)

	got, err := s.repo.Get(ctx, "order", "1")
	s.Require().NoError(err)
	s.JSONEq(`{"n":"b"}`, string(got.Data), "the losing write must not land")
}

func (s *EntityRepositorySuite) TestListIsScopedToTypeAndCarriesEntityID() {
	ctx := context.Background()
	s.Require().NoError(s.repo.Save(ctx, marshaled("order", "1", 0, `{"n":"a"}`)))
	s.Require().NoError(s.repo.Save(ctx, marshaled("offer", "1", 0, `{"n":"b"}`)))

	got, err := s.repo.List(ctx, "order", nil, ember.Sort{})
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("1", got[0].ID)
	s.Equal("order", got[0].Type)
}

// The "id" filter path must resolve to the entity id, not the _id surrogate.
func (s *EntityRepositorySuite) TestListFiltersByEntityID() {
	ctx := context.Background()
	s.Require().NoError(s.repo.Save(ctx, marshaled("order", "1", 0, `{"n":"a"}`)))
	s.Require().NoError(s.repo.Save(ctx, marshaled("order", "2", 0, `{"n":"b"}`)))

	got, err := s.repo.List(ctx, "order", ember.Eq("id", "2"), ember.Sort{})
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("2", got[0].ID)
}

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)
