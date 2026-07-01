package embermem

import (
	"context"
	"testing"

	"github.com/klemen-forstneric/ember"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	got, err := r.List(ctx, "t", ember.And(ember.Eq("user", "a"), ember.Eq("kind", "x")), ember.Sort{})
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

	asc, err := r.List(ctx, "t", nil, ember.Asc("created_at"))
	require.NoError(t, err)
	assert.Equal(t, []string{"2", "3", "1"}, []string{asc[0].ID, asc[1].ID, asc[2].ID})

	desc, err := r.List(ctx, "t", nil, ember.Desc("created_at"))
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "3", "2"}, []string{desc[0].ID, desc[1].ID, desc[2].ID})
}

func TestListNegationAndExistence(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("1", "t", 1, `{"user":"a"}`)))
	require.NoError(t, r.Save(ctx, me("2", "t", 1, `{"user":"b"}`)))
	require.NoError(t, r.Save(ctx, me("3", "t", 1, `{}`)))

	notA, err := r.List(ctx, "t", ember.Not(ember.Eq("user", "a")), ember.Sort{})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"2", "3"}, []string{notA[0].ID, notA[1].ID})

	hasUser, err := r.List(ctx, "t", ember.Exists("user", true), ember.Sort{})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "2"}, []string{hasUser[0].ID, hasUser[1].ID})
}

// Sort is lexical (text) ordering, matching the SQL backend's uncast jsonb text
// extraction — numeric values order as text ("10" < "2" < "9"), NOT numerically.
func TestListSortIsLexical(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{"n":9}`)))
	require.NoError(t, r.Save(ctx, me("b", "t", 1, `{"n":10}`)))
	require.NoError(t, r.Save(ctx, me("c", "t", 1, `{"n":2}`)))

	got, err := r.List(ctx, "t", nil, ember.Asc("n"))
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, []string{"b", "c", "a"}, []string{got[0].ID, got[1].ID, got[2].ID})
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
