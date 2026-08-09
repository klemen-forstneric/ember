package mongo

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
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

	got, err := s.repo.List(ctx, "order", nil, ember.Sort{}, ember.Unpaged())
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

	got, err := s.repo.List(ctx, "order", ember.Eq("id", "2"), ember.Sort{}, ember.Unpaged())
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("2", got[0].ID)
}

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)

func TestListPagingLimitAndSkip(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []interface{}{
		makeNumEntity(1, "id1"),
		makeNumEntity(2, "id2"),
		makeNumEntity(3, "id3"),
	})
	require.NoError(t, err)

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	first, err := repo.List(ctx, "fake", nil, ember.Asc("n").Numeric(), ember.Limit(2))
	require.NoError(t, err)
	require.Equal(t, []float64{1, 2}, nNumbers(first))

	second, err := repo.List(ctx, "fake", nil, ember.Asc("n").Numeric(), ember.Limit(2).Skip(2))
	require.NoError(t, err)
	require.Equal(t, []float64{3}, nNumbers(second))
}

func TestListPagingUnsortedKeyset(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []interface{}{
		makeNumEntity(1, "id1"),
		makeNumEntity(2, "id2"),
		makeNumEntity(3, "id3"),
	})
	require.NoError(t, err)

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	first, err := repo.List(ctx, "fake", nil, ember.Unsorted(), ember.Limit(2))
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Equal(t, []string{"id1", "id2"}, []string{first[0].ID, first[1].ID})

	second, err := repo.List(ctx, "fake", nil, ember.Unsorted(), ember.Limit(2).After(nil, first[1].ID))
	require.NoError(t, err)
	require.Equal(t, []string{"id3"}, []string{second[0].ID})
}

func TestListPagingKeysetAcrossTie(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []interface{}{
		makeNumEntity(5, "idA"),
		makeNumEntity(5, "idB"),
		makeNumEntity(5, "idC"),
		makeNumEntity(6, "idD"),
	})
	require.NoError(t, err)

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	sort := ember.Asc("n").Numeric()
	var seen []string
	page := ember.Limit(2)
	for {
		got, err := repo.List(ctx, "fake", nil, sort, page)
		require.NoError(t, err)
		if len(got) == 0 {
			break
		}
		for _, m := range got {
			seen = append(seen, m.ID)
		}
		last := got[len(got)-1]
		var d map[string]float64
		require.NoError(t, json.Unmarshal(last.Data, &d))
		page = ember.Limit(2).After(d["n"], last.ID)
		if len(got) < 2 {
			break
		}
	}

	require.Equal(t, []string{"idA", "idB", "idC", "idD"}, seen)
}

func TestListPagingSortedCursorNeedsValue(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	_, err = repo.List(ctx, "fake", nil, ember.Asc("n").Numeric(), ember.Limit(2).After(nil, "idA"))
	require.ErrorIs(t, err, ember.ErrInvalidCursor)
}
