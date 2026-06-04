package postgres

import (
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

func TestBuildPredicate(t *testing.T) {
	tests := []struct {
		name     string
		filter   ember.Filter
		wantSQL  string
		wantArgs []any
	}{
		{"eq string data path", ember.Eq("status", "open"), "(data#>>'{status}' IS NOT NULL AND data#>>'{status}' = ?)", []any{"open"}},
		{"nested data path", ember.Eq("address.city", "NYC"), "(data#>>'{address,city}' IS NOT NULL AND data#>>'{address,city}' = ?)", []any{"NYC"}},
		{"reserved id", ember.Eq("id", "x"), "id = ?", []any{"x"}},
		{"reserved version", ember.Gt("version", 5), "version > ?", []any{5}},
		{"gt numeric cast", ember.Gt("total", 4200), "(data#>>'{total}' IS NOT NULL AND (data#>>'{total}')::numeric > ?)", []any{4200}},
		{"ne", ember.Ne("status", "open"), "(data#>>'{status}' IS NOT NULL AND data#>>'{status}' <> ?)", []any{"open"}},
		{"ne reserved version", ember.Ne("version", 5), "version <> ?", []any{5}},
		{"in", ember.In("region", "EU", "UK"), "(data#>>'{region}' IS NOT NULL AND data#>>'{region}' IN (?,?))", []any{"EU", "UK"}},
		{"in empty is false", ember.In("region"), "FALSE", nil},
		{"in numeric cast", ember.In("price", 100, 200), "(data#>>'{price}' IS NOT NULL AND (data#>>'{price}')::numeric IN (?,?))", []any{100, 200}},
		{"exists true", ember.Exists("status", true), "data#>>'{status}' IS NOT NULL", nil},
		{"exists false", ember.Exists("status", false), "data#>>'{status}' IS NULL", nil},
		{"not", ember.Not(ember.Eq("status", "open")), "NOT ((data#>>'{status}' IS NOT NULL AND data#>>'{status}' = ?))", []any{"open"}},
		{
			"and",
			ember.And(ember.Eq("status", "open"), ember.Gt("total", 100)),
			"((data#>>'{status}' IS NOT NULL AND data#>>'{status}' = ?) AND (data#>>'{total}' IS NOT NULL AND (data#>>'{total}')::numeric > ?))",
			[]any{"open", 100},
		},
		{
			"or",
			ember.Or(ember.Eq("a", "1"), ember.Eq("b", "2")),
			"((data#>>'{a}' IS NOT NULL AND data#>>'{a}' = ?) OR (data#>>'{b}' IS NOT NULL AND data#>>'{b}' = ?))",
			[]any{"1", "2"},
		},
		{
			"not of and",
			ember.Not(ember.And(ember.Eq("a", "1"), ember.Eq("b", "2"))),
			"NOT (((data#>>'{a}' IS NOT NULL AND data#>>'{a}' = ?) AND (data#>>'{b}' IS NOT NULL AND data#>>'{b}' = ?)))",
			[]any{"1", "2"},
		},
		{"time normalized to rfc3339nano", ember.Eq("createdAt", time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)), "(data#>>'{createdAt}' IS NOT NULL AND data#>>'{createdAt}' = ?)", []any{"2024-01-02T03:04:05Z"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, err := buildPredicate(tt.filter)
			require.NoError(t, err)
			gotSQL, gotArgs, err := pred.ToSql()
			require.NoError(t, err, "ToSql")
			assert.Equal(t, tt.wantSQL, gotSQL, "sql")
			assert.Equal(t, tt.wantArgs, gotArgs, "args")
		})
	}
}

func TestBuildPredicateNilMatchesAll(t *testing.T) {
	pred, err := buildPredicate(nil)
	require.NoError(t, err)
	assert.Nil(t, pred, "nil filter should yield nil predicate")
}

func TestBuildPredicateUnsupportedValue(t *testing.T) {
	_, err := buildPredicate(ember.Eq("status", []string{"nope"}))
	assert.ErrorIs(t, err, ember.ErrUnsupportedFilter)
}

// TestListQueryPlaceholders pins the List seam: the type scope and the filter
// predicate share one statement, and squirrel renumbers `?` to sequential `$N`
// across both (type first, then the filter args in order).
func TestListQueryPlaceholders(t *testing.T) {
	pred, err := buildPredicate(ember.And(ember.Eq("status", "open"), ember.Gt("total", 100)))
	require.NoError(t, err, "buildPredicate")

	gotSQL, gotArgs, err := psql.
		Select("id", "version", "data").
		From("entities").
		Where(sq.Eq{"type": "order"}).
		Where(pred).
		ToSql()
	require.NoError(t, err, "ToSql")

	wantSQL := "SELECT id, version, data FROM entities WHERE type = $1 AND " +
		"((data#>>'{status}' IS NOT NULL AND data#>>'{status}' = $2) AND " +
		"(data#>>'{total}' IS NOT NULL AND (data#>>'{total}')::numeric > $3))"
	assert.Equal(t, wantSQL, gotSQL, "sql")
	assert.Equal(t, []any{"order", "open", 100}, gotArgs, "args")
}

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)
