package embertest

import (
	"context"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ids(ms []*ember.MarshaledEntity) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

func me(id, typ string, ver uint64, data string) *ember.MarshaledEntity {
	// ver is the desired stored value; produce initial=ver-1 + one Inc so Save
	// sees initial==0 for a fresh insert (ver==1) or initial==n-1 for an update.
	v := ember.NewVersion(ver - 1).Inc()
	return &ember.MarshaledEntity{ID: id, Type: typ, Version: v, Data: []byte(data)}
}

func TestSaveOptimisticVersion(t *testing.T) {
	r := New()
	ctx := context.Background()
	// initial 0 -> stored value 1
	require.NoError(t, r.Save(ctx, &ember.MarshaledEntity{ID: "1", Type: "t", Version: ember.NewVersion(0).Inc()}))
	// stale save (initial 0 again) conflicts
	require.ErrorIs(t, r.Save(ctx, &ember.MarshaledEntity{ID: "1", Type: "t", Version: ember.NewVersion(0).Inc()}), ember.ErrVersionConflict)
}

// A Get then re-Save of the same entity must not self-conflict: Get must return
// a normalized version (initial == stored value) so the caller's next Inc+Save
// filters on the just-persisted value — matching the real backends.
func TestGetThenResaveRoundTrip(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, &ember.MarshaledEntity{ID: "1", Type: "t", Version: ember.NewVersion(0).Inc()}))

	got, err := r.Get(ctx, "t", "1")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), got.Version.Value())
	assert.Equal(t, uint64(1), got.Version.Initial()) // normalized on store

	got.Version = got.Version.Inc() // caller mutates + bumps
	require.NoError(t, r.Save(ctx, got))
}

func TestListFilterEqAndAnd(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("1", "t", 1, `{"user":"a","kind":"x"}`)))
	require.NoError(t, r.Save(ctx, me("2", "t", 1, `{"user":"a","kind":"y"}`)))
	require.NoError(t, r.Save(ctx, me("3", "t", 1, `{"user":"b","kind":"x"}`)))

	got, err := r.List(ctx, "t", ember.And(ember.Eq("user", "a"), ember.Eq("kind", "x")), ember.Sort{}, ember.Unpaged())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "1", got[0].ID)
}

func TestListSort(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("1", "t", 1, `{"created_at":"2026-01-03"}`)))
	require.NoError(t, r.Save(ctx, me("2", "t", 1, `{"created_at":"2026-01-01"}`)))
	require.NoError(t, r.Save(ctx, me("3", "t", 1, `{"created_at":"2026-01-02"}`)))

	asc, err := r.List(ctx, "t", nil, ember.Asc("created_at"), ember.Unpaged())
	require.NoError(t, err)
	assert.Equal(t, []string{"2", "3", "1"}, []string{asc[0].ID, asc[1].ID, asc[2].ID})

	desc, err := r.List(ctx, "t", nil, ember.Desc("created_at"), ember.Unpaged())
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "3", "2"}, []string{desc[0].ID, desc[1].ID, desc[2].ID})
}

func TestListNegationAndExistence(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("1", "t", 1, `{"user":"a"}`)))
	require.NoError(t, r.Save(ctx, me("2", "t", 1, `{"user":"b"}`)))
	require.NoError(t, r.Save(ctx, me("3", "t", 1, `{}`)))

	notA, err := r.List(ctx, "t", ember.Not(ember.Eq("user", "a")), ember.Sort{}, ember.Unpaged())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"2", "3"}, []string{notA[0].ID, notA[1].ID})

	hasUser, err := r.List(ctx, "t", ember.Exists("user", true), ember.Sort{}, ember.Unpaged())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "2"}, []string{hasUser[0].ID, hasUser[1].ID})
}

func TestListSortLexicalVsNumeric(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{"n":9}`)))
	require.NoError(t, r.Save(ctx, me("b", "t", 1, `{"n":10}`)))
	require.NoError(t, r.Save(ctx, me("c", "t", 1, `{"n":2}`)))

	lex, err := r.List(ctx, "t", nil, ember.Asc("n"), ember.Unpaged())
	require.NoError(t, err)
	require.Len(t, lex, 3)
	assert.Equal(t, []string{"b", "c", "a"}, []string{lex[0].ID, lex[1].ID, lex[2].ID})

	num, err := r.List(ctx, "t", nil, ember.Asc("n").Numeric(), ember.Unpaged())
	require.NoError(t, err)
	require.Len(t, num, 3)
	assert.Equal(t, []string{"c", "a", "b"}, []string{num[0].ID, num[1].ID, num[2].ID})

	desc, err := r.List(ctx, "t", nil, ember.Desc("n").Numeric(), ember.Unpaged())
	require.NoError(t, err)
	require.Len(t, desc, 3)
	assert.Equal(t, []string{"b", "a", "c"}, []string{desc[0].ID, desc[1].ID, desc[2].ID})
}

// Data returned from the store must not alias stored state: mutating a returned
// Data slice must not corrupt the repository.
func TestListGetDoNotAliasData(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("1", "t", 1, `{"k":"v"}`)))

	got, err := r.Get(ctx, "t", "1")
	require.NoError(t, err)
	require.NotEmpty(t, got.Data)
	got.Data[0] = 'X' // mutate the returned slice in place

	again, err := r.Get(ctx, "t", "1")
	require.NoError(t, err)
	assert.Equal(t, byte('{'), again.Data[0]) // store uncorrupted
}

func TestListPagingLimitAndOffset(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{"n":1}`)))
	require.NoError(t, r.Save(ctx, me("b", "t", 1, `{"n":2}`)))
	require.NoError(t, r.Save(ctx, me("c", "t", 1, `{"n":3}`)))

	sort := ember.Asc("n").Numeric()

	first, err := r.List(ctx, "t", nil, sort, ember.Limit(2))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, ids(first))

	second, err := r.List(ctx, "t", nil, sort, ember.Limit(2).Skip(2))
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids(second))

	past, err := r.List(ctx, "t", nil, sort, ember.Limit(2).Skip(99))
	require.NoError(t, err)
	assert.Empty(t, past)
}

func TestListPagingKeysetAcrossTie(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("idA", "t", 1, `{"n":5}`)))
	require.NoError(t, r.Save(ctx, me("idB", "t", 1, `{"n":5}`)))
	require.NoError(t, r.Save(ctx, me("idC", "t", 1, `{"n":5}`)))
	require.NoError(t, r.Save(ctx, me("idD", "t", 1, `{"n":6}`)))

	sort := ember.Asc("n").Numeric()

	first, err := r.List(ctx, "t", nil, sort, ember.Limit(2))
	require.NoError(t, err)
	assert.Equal(t, []string{"idA", "idB"}, ids(first))

	second, err := r.List(ctx, "t", nil, sort, ember.Limit(2).After(float64(5), "idB"))
	require.NoError(t, err)
	assert.Equal(t, []string{"idC", "idD"}, ids(second))

	third, err := r.List(ctx, "t", nil, sort, ember.Limit(2).After(float64(6), "idD"))
	require.NoError(t, err)
	assert.Empty(t, third)
}

func TestListPagingUnsortedKeyset(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{}`)))
	require.NoError(t, r.Save(ctx, me("b", "t", 1, `{}`)))
	require.NoError(t, r.Save(ctx, me("c", "t", 1, `{}`)))

	first, err := r.List(ctx, "t", nil, ember.Unsorted(), ember.Limit(2))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, ids(first))

	second, err := r.List(ctx, "t", nil, ember.Unsorted(), ember.Limit(2).After(nil, "b"))
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids(second))
}

func TestListPagingUnsortedIgnoresDescendingDirection(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{}`)))
	require.NoError(t, r.Save(ctx, me("b", "t", 1, `{}`)))
	require.NoError(t, r.Save(ctx, me("c", "t", 1, `{}`)))

	s := ember.Sort{Direction: ember.Descending}

	first, err := r.List(ctx, "t", nil, s, ember.Limit(2))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, ids(first))

	second, err := r.List(ctx, "t", nil, s, ember.Limit(2).After(nil, "b"))
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids(second))
}

func TestListPagingSortedCursorNeedsValue(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{"n":1}`)))

	_, err := r.List(ctx, "t", nil, ember.Asc("n").Numeric(), ember.Limit(2).After(nil, "a"))
	require.ErrorIs(t, err, ember.ErrInvalidCursor)
}

func TestListPagingSortedCursorRejectsUnsupportedValue(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{"n":1}`)))

	_, err := r.List(ctx, "t", nil, ember.Asc("n").Numeric(), ember.Limit(2).After(time.Second, "a"))
	require.ErrorIs(t, err, ember.ErrInvalidCursor)
}

func TestListPagingMissingPathTiebreakIsDeterministic(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("n1", "t", 1, `{"n":1}`)))
	require.NoError(t, r.Save(ctx, me("n2", "t", 1, `{"n":2}`)))
	require.NoError(t, r.Save(ctx, me("m1", "t", 1, `{}`)))
	require.NoError(t, r.Save(ctx, me("m2", "t", 1, `{}`)))

	sort := ember.Asc("n").Numeric()

	var walk []string
	for i := 0; i < 4; i++ {
		page, err := r.List(ctx, "t", nil, sort, ember.Limit(1).Skip(i))
		require.NoError(t, err)
		walk = append(walk, ids(page)...)
	}
	assert.Equal(t, []string{"n1", "n2", "m1", "m2"}, walk)

	for i := 0; i < 5; i++ {
		page, err := r.List(ctx, "t", nil, sort, ember.Limit(1).Skip(i))
		require.NoError(t, err)
		if i < len(walk) {
			assert.Equal(t, []string{walk[i]}, ids(page))
		} else {
			assert.Empty(t, page)
		}
	}

	past, err := r.List(ctx, "t", nil, sort, ember.Limit(2).After(float64(2), "n2"))
	require.NoError(t, err)
	assert.Empty(t, past)
}

func TestListPagingDescendingKeysetAcrossTie(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("idA", "t", 1, `{"n":5}`)))
	require.NoError(t, r.Save(ctx, me("idB", "t", 1, `{"n":5}`)))
	require.NoError(t, r.Save(ctx, me("idC", "t", 1, `{"n":5}`)))
	require.NoError(t, r.Save(ctx, me("idD", "t", 1, `{"n":6}`)))

	sort := ember.Desc("n").Numeric()

	all, err := r.List(ctx, "t", nil, sort, ember.Limit(4))
	require.NoError(t, err)
	assert.Equal(t, []string{"idD", "idC", "idB", "idA"}, ids(all))
}
