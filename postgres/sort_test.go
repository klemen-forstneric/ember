package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

func TestOrderBy(t *testing.T) {
	tests := []struct {
		name string
		sort ember.Sort
		want []string
	}{
		{"unsorted", ember.Unsorted(), nil},
		{"lexical asc", ember.AscLex("created_at"), []string{"data#>>'{created_at}' ASC"}},
		{"lexical desc", ember.DescLex("created_at"), []string{"data#>>'{created_at}' DESC"}},
		{"numeric asc", ember.AscNum("seq"), []string{"(data#>>'{seq}')::numeric ASC"}},
		{"numeric desc", ember.DescNum("seq"), []string{"(data#>>'{seq}')::numeric DESC"}},
		{"nested path", ember.Asc("job.id"), []string{"data#>>'{job,id}' ASC"}},
		{"reserved id", ember.Asc("id"), []string{"id ASC"}},
		{"reserved version ignores ordering", ember.AscNum("version"), []string{"version ASC"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, orderBy(tt.sort, false))
		})
	}
}

func TestOrderByPagedAppendsTiebreak(t *testing.T) {
	assert.Equal(t, []string{"id ASC"}, orderBy(ember.Unsorted(), true))
	assert.Equal(t, []string{"data#>>'{created_at}' ASC", "id ASC"}, orderBy(ember.Asc("created_at"), true))
	assert.Equal(t, []string{"(data#>>'{seq}')::numeric DESC", "id DESC"}, orderBy(ember.DescNum("seq"), true))
}

func TestSeekPredicate(t *testing.T) {
	tests := []struct {
		name     string
		sort     ember.Sort
		cursor   ember.Cursor
		wantSQL  string
		wantArgs []any
	}{
		{
			"unsorted",
			ember.Unsorted(),
			ember.Cursor{ID: "pay_abc"},
			"id > ?",
			[]any{"pay_abc"},
		},
		{
			"lexical asc",
			ember.AscLex("settle_by"),
			ember.Cursor{Value: "2026-08-09", ID: "pay_abc"},
			"(data#>>'{settle_by}', id) > (?, ?)",
			[]any{"2026-08-09", "pay_abc"},
		},
		{
			"numeric desc",
			ember.DescNum("seq"),
			ember.Cursor{Value: int64(7), ID: "m1"},
			"((data#>>'{seq}')::numeric, id) < (?::numeric, ?)",
			[]any{int64(7), "m1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, err := seekPredicate(tt.sort, tt.cursor)
			require.NoError(t, err)

			gotSQL, gotArgs, err := pred.ToSql()
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, gotSQL)
			assert.Equal(t, tt.wantArgs, gotArgs)
		})
	}
}

func TestSeekPredicateSortedCursorNeedsValue(t *testing.T) {
	_, err := seekPredicate(ember.AscNum("seq"), ember.Cursor{ID: "m1"})
	require.ErrorIs(t, err, ember.ErrInvalidCursor)
}

func TestSeekPredicateCursorValueInvalidWrapsErrInvalidCursor(t *testing.T) {
	_, err := seekPredicate(ember.AscNum("n"), ember.Cursor{Value: time.Duration(1), ID: "m1"})
	require.ErrorIs(t, err, ember.ErrInvalidCursor)
}

func TestSeekPredicateUnsortedIgnoresDirection(t *testing.T) {
	pred, err := seekPredicate(ember.Sort{Direction: ember.Descending}, ember.Cursor{ID: "pay_abc"})
	require.NoError(t, err)

	gotSQL, _, err := pred.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "id > ?", gotSQL)
}
