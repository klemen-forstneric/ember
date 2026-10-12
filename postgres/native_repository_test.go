package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/stretchr/testify/require"
)

type nativeState string

// The domain includes private and transient state and has no SQL tags. Only the
// mapper knows its representation; the repository must not inspect this entity.
type nativeItem struct {
	ember.EntityRoot
	owner     string
	Rank      int64
	Active    bool
	State     nativeState
	Optional  *int64
	Alias     string
	At        time.Time
	Later     *time.Time
	Transient string
	secret    string
}

func (*nativeItem) Type() string { return "native_item" }

type nativeRow struct {
	Account  string     `db:"owner"`
	Position int64      `db:"rank"`
	Active   bool       `db:"active"`
	State    string     `db:"state"`
	Optional *int64     `db:"optional"`
	Alias    string     `db:"alias,nullzero"`
	At       time.Time  `db:"at"`
	Later    *time.Time `db:"later"`
}
type nativeMapper struct{}

func (nativeMapper) Marshal(_ context.Context, e *nativeItem) (nativeRow, error) {
	return nativeRow{Account: e.owner, Position: e.Rank, Active: e.Active,
		State: string(e.State), Optional: e.Optional, Alias: e.Alias, At: e.At, Later: e.Later}, nil
}
func (nativeMapper) Unmarshal(_ context.Context, row Row[nativeRow]) (*nativeItem, error) {
	d := row.Data
	return &nativeItem{EntityRoot: ember.NewEntityRoot(row.ID), owner: d.Account,
		Rank: d.Position, Active: d.Active, State: nativeState(d.State),
		Optional: d.Optional, Alias: d.Alias, At: d.At, Later: d.Later}, nil
}

type unmappedRow struct{ Value string }
type nestedRow struct {
	Value map[string]string `db:"value"`
}
type duplicateColumnRow struct {
	Value string `db:"version"`
}
type testMapper[R any] struct{}

func (testMapper[R]) Marshal(context.Context, *nativeItem) (R, error) { var zero R; return zero, nil }
func (testMapper[R]) Unmarshal(_ context.Context, row Row[R]) (*nativeItem, error) {
	return &nativeItem{EntityRoot: ember.NewEntityRoot(row.ID)}, nil
}

func TestNativeMappingRejectsSilentOmissionsAndUnsafeNames(t *testing.T) {
	_, err := NewRepository(nil, "items", testMapper[unmappedRow]{})
	require.ErrorContains(t, err, "needs a db column tag")
	_, err = NewRepository(nil, "items", testMapper[nestedRow]{})
	require.ErrorContains(t, err, "unsupported field")
	_, err = NewRepository(nil, "items", testMapper[duplicateColumnRow]{})
	require.ErrorContains(t, err, "duplicate column")
	_, err = NewRepository(nil, "items; DROP TABLE items", nativeMapper{})
	require.ErrorContains(t, err, "invalid identifier")
	_, err = NewRepository(nil, "items", testMapper[string]{})
	require.ErrorContains(t, err, "row type must be a struct")
	_, err = NewRepository[*nativeItem, nativeRow](nil, "items", nil)
	require.ErrorContains(t, err, "row mapper is nil")
	_, err = NewRepository(nil, "items", (*nativeMapper)(nil))
	require.ErrorContains(t, err, "row mapper is nil")
}

func nativeItems(t *testing.T) (*sql.DB, *Repository[*nativeItem]) {
	t.Helper()
	pool := nativeTestDB(t)
	_, err := pool.Exec(`CREATE TABLE items (
		id text PRIMARY KEY, version bigint NOT NULL, owner text NOT NULL,
		rank bigint NOT NULL, active boolean NOT NULL, state text NOT NULL,
		optional bigint, alias text, at timestamptz NOT NULL, later timestamptz
	); CREATE INDEX items_listing ON items(owner, rank, id)`)
	require.NoError(t, err)
	r, err := NewRepository(NewDB(pool), "items", nativeMapper{})
	require.NoError(t, err)
	return pool, r
}

func TestNativeMappingAndConflictContract(t *testing.T) {
	_, r := nativeItems(t)
	ctx := context.Background()
	at := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.FixedZone("offset", 3600))
	n := int64(12)
	e := &nativeItem{EntityRoot: ember.NewEntityRoot("one"), owner: "owner", Rank: 42,
		Active: true, State: "open", Optional: &n, At: at, Later: &at, Transient: "ignored"}
	require.NoError(t, r.Save(ctx, e, e.Version().Inc()))
	require.Zero(t, e.Version().Value())
	require.Equal(t, at, e.At)
	got, err := r.Get(ctx, e.ID())
	require.NoError(t, err)
	require.Equal(t, int64(42), got.Rank)
	require.True(t, got.Active)
	require.Equal(t, nativeState("open"), got.State)
	require.Equal(t, &n, got.Optional)
	require.Equal(t, at.UTC().Truncate(time.Microsecond), got.At)
	require.Equal(t, got.At, *got.Later)
	require.Empty(t, got.Alias)
	require.Empty(t, got.Transient)
	require.ErrorIs(t, r.Save(ctx, e, e.Version().Inc()), ember.ErrVersionConflict)
	got.Optional, got.Later = nil, nil
	got.Alias = "present"
	require.NoError(t, r.Save(ctx, got, got.Version().Inc()))
	require.ErrorIs(t, r.Save(ctx, got, got.Version().Inc()), ember.ErrVersionConflict)
	got, err = r.Get(ctx, e.ID())
	require.NoError(t, err)
	require.Nil(t, got.Optional)
	require.Nil(t, got.Later)
	require.Equal(t, "present", got.Alias)
	_, err = r.Get(ctx, "missing")
	require.ErrorIs(t, err, ember.ErrEntityNotFound)
	missing := &nativeItem{EntityRoot: ember.NewEntityRoot("missing")}
	require.ErrorIs(t, r.Save(ctx, missing, ember.NewVersion(1).Inc()), ember.ErrVersionConflict)
}

func TestNativeFiltersAndPagination(t *testing.T) {
	_, r := nativeItems(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Microsecond)
	for i, id := range []string{"a", "b", "c", "d", "e"} {
		e := &nativeItem{EntityRoot: ember.NewEntityRoot(id), owner: "owner", Rank: int64(i / 2), Active: i%2 == 0, State: "open", At: at.Add(time.Duration(i) * time.Microsecond)}
		if i < 3 {
			n := int64(i / 2)
			e.Optional = &n
		}
		require.NoError(t, r.Save(ctx, e, e.Version().Inc()))
	}
	for _, tc := range []struct {
		f   ember.Filter
		ids []string
	}{
		{ember.And(ember.Eq("owner", "owner"), ember.Gte("rank", 1)), []string{"c", "d", "e"}},
		{ember.Or(ember.Eq("active", false), ember.Eq("rank", 2)), []string{"b", "d", "e"}},
		{ember.In("rank", 0, 2), []string{"a", "b", "e"}},
		{ember.In("rank"), nil},
		{ember.And(), []string{"a", "b", "c", "d", "e"}},
		{ember.Or(), nil},
		{ember.Not(ember.Eq("optional", 0)), []string{"c", "d", "e"}},
		{ember.Exists("optional", false), []string{"d", "e"}},
		{ember.Eq("state", nativeState("open")), []string{"a", "b", "c", "d", "e"}},
		{ember.Gt("at", at.Add(time.Microsecond)), []string{"c", "d", "e"}},
	} {
		got, err := r.List(ctx, tc.f, ember.AscLex("id"))
		require.NoError(t, err)
		require.Equal(t, tc.ids, nativeIDs(got))
	}
	for _, tc := range []struct {
		sort ember.Sort
		want []string
	}{
		{ember.AscNum("rank"), []string{"a", "b", "c", "d", "e"}},
		{ember.DescNum("rank"), []string{"e", "d", "c", "b", "a"}},
		{ember.AscNum("optional"), []string{"a", "b", "c", "d", "e"}},
		{ember.DescNum("optional"), []string{"e", "d", "c", "b", "a"}},
		{ember.AscLex("at"), []string{"a", "b", "c", "d", "e"}},
		{ember.Unsorted(), []string{"a", "b", "c", "d", "e"}},
	} {
		page := ember.Limit(1)
		var ids []string
		for range 7 {
			got, err := r.ListPage(ctx, nil, tc.sort, page)
			require.NoError(t, err)
			if len(got) == 0 {
				break
			}
			last := got[len(got)-1]
			ids = append(ids, last.ID())
			var value any
			switch tc.sort.Path {
			case "rank":
				value = last.Rank
			case "optional":
				if last.Optional != nil {
					value = *last.Optional
				}
			case "at":
				value = last.At
			}
			// Exercise cursor tokens as they cross a request boundary.
			b, err := (ember.Cursor{ID: last.ID(), Value: value}).MarshalText()
			require.NoError(t, err)
			var cursor ember.Cursor
			require.NoError(t, cursor.UnmarshalText(b))
			page = ember.Limit(1).AfterCursor(cursor)
		}
		require.Equal(t, tc.want, ids)
	}
	got, err := r.ListPage(ctx, nil, ember.Unsorted(), ember.Limit(2).Skip(2))
	require.NoError(t, err)
	require.Equal(t, []string{"c", "d"}, nativeIDs(got))
	_, err = r.List(ctx, ember.Eq("owner'; DROP TABLE items; --", "x"), ember.Unsorted())
	require.ErrorIs(t, err, ember.ErrUnsupportedFilter)
	_, err = r.ListPage(ctx, nil, ember.AscNum("rank"), ember.Limit(1).After(nil, "a"))
	require.ErrorIs(t, err, ember.ErrInvalidCursor)
	_, err = r.ListPage(ctx, nil, ember.Unsorted(), ember.Limit(-1))
	require.ErrorIs(t, err, ember.ErrInvalidPage)
}

func nativeIDs(items []*nativeItem) (out []string) {
	for _, e := range items {
		out = append(out, e.ID())
	}
	return
}

func TestNativeListingUsesCompoundIndex(t *testing.T) {
	pool, r := nativeItems(t)
	_, err := pool.Exec(`INSERT INTO items(id,version,owner,rank,active,state,at)
		SELECT n::text,1,'owner',n,true,'open',now() FROM generate_series(1,20000) n; ANALYZE items`)
	require.NoError(t, err)
	query, args, err := r.listQuery(ember.Eq("owner", "owner"), ember.AscNum("rank"), ember.Limit(20).After(18000, "18000"))
	require.NoError(t, err)
	var plan string
	require.NoError(t, pool.QueryRow(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) `+query, args...).Scan(&plan))
	require.Contains(t, plan, "items_listing")
	require.NotContains(t, plan, `"Seq Scan"`)
	require.NotContains(t, plan, `"Node Type": "Sort"`)
	require.Contains(t, plan, `"Index Cond"`)
}
