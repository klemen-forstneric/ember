package postgres

import (
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/klemen-forstneric/ember"
)

// buildPredicate translates a Filter into a squirrel.Sqlizer. A nil filter
// returns (nil, nil); the caller omits the predicate (match all). Placeholders
// are left as `?` and renumbered to `$N` by the statement's PlaceholderFormat.
func buildPredicate(f ember.Filter) (sq.Sqlizer, error) {
	if f == nil {
		return nil, nil
	}
	switch n := f.(type) {
	case ember.Comparison:
		return comparison(n)
	case ember.Membership:
		return membership(n)
	case ember.Existence:
		return existence(n), nil
	case ember.Conjunction:
		return composite(n.Filters, true)
	case ember.Disjunction:
		return composite(n.Filters, false)
	case ember.Negation:
		inner, err := buildPredicate(n.Filter)
		if err != nil {
			return nil, err
		}
		// squirrel expands the nested Sqlizer arg and merges its bind args.
		return sq.Expr("NOT (?)", inner), nil
	default:
		return nil, fmt.Errorf("%w: unknown node %T", ember.ErrUnsupportedFilter, f)
	}
}

func comparison(c ember.Comparison) (sq.Sqlizer, error) {
	v, err := normalizeValue(c.Value)
	if err != nil {
		return nil, err
	}
	col, reserved := column(c.Path)
	if reserved {
		return compare(col, c.Op, v) // reserved columns are NOT NULL by schema
	}
	cmp, err := compare(castFor(col, c.Value), c.Op, v)
	if err != nil {
		return nil, err
	}
	// Two-valued guard: a missing/null jsonb path makes the leaf strictly false
	// (instead of SQL NULL), so Not over it is a clean complement.
	return sq.And{sq.Expr(col + " IS NOT NULL"), cmp}, nil
}

// compare keys a squirrel comparison off lhs, which is either a reserved column
// name or a jsonb extraction/cast expression — squirrel interpolates it verbatim.
func compare(lhs string, op ember.Operator, v any) (sq.Sqlizer, error) {
	switch op {
	case ember.OpEq:
		return sq.Eq{lhs: v}, nil
	case ember.OpNe:
		return sq.NotEq{lhs: v}, nil
	case ember.OpGt:
		return sq.Gt{lhs: v}, nil
	case ember.OpGte:
		return sq.GtOrEq{lhs: v}, nil
	case ember.OpLt:
		return sq.Lt{lhs: v}, nil
	case ember.OpLte:
		return sq.LtOrEq{lhs: v}, nil
	default:
		return nil, fmt.Errorf("%w: operator %d", ember.ErrUnsupportedFilter, op)
	}
}

func membership(m ember.Membership) (sq.Sqlizer, error) {
	if len(m.Values) == 0 {
		return sq.Expr("FALSE"), nil
	}
	vals, err := normalizeValues(m.Values)
	if err != nil {
		return nil, err
	}
	col, reserved := column(m.Path)
	if reserved {
		return sq.Eq{col: vals}, nil // squirrel expands a slice to IN (?, ?, …)
	}
	return sq.And{
		sq.Expr(col + " IS NOT NULL"),
		sq.Eq{castFor(col, m.Values[0]): vals},
	}, nil
}

func existence(e ember.Existence) sq.Sqlizer {
	col, _ := column(e.Path)
	if e.Exists {
		return sq.Expr(col + " IS NOT NULL")
	}
	return sq.Expr(col + " IS NULL")
}

func composite(fs []ember.Filter, and bool) (sq.Sqlizer, error) {
	parts := make([]sq.Sqlizer, 0, len(fs))
	for _, f := range fs {
		p, err := buildPredicate(f)
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	if len(parts) == 0 {
		// squirrel renders an empty And/Or as empty SQL, so emit the identity.
		if and {
			return sq.Expr("TRUE"), nil
		}
		return sq.Expr("FALSE"), nil
	}
	if and {
		return sq.And(parts), nil
	}
	return sq.Or(parts), nil
}

// column maps a filter path to a SQL column expression. Reserved metadata paths
// map to real columns; everything else extracts text from the jsonb data column.
// Path segments must not contain commas, braces, or double-quote characters.
func column(path string) (expr string, reserved bool) {
	switch path {
	case "id", "type", "version":
		return path, true
	default:
		segs := strings.Split(path, ".")
		return fmt.Sprintf("data#>>'{%s}'", strings.Join(segs, ",")), false
	}
}

// castFor wraps a jsonb text expression with a cast so non-string comparisons
// behave numerically/logically rather than lexically.
func castFor(col string, v any) string {
	switch v.(type) {
	case bool:
		return "(" + col + ")::boolean"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return "(" + col + ")::numeric"
	default:
		return col
	}
}

// normalizeValue validates a filter value and converts time.Time to RFC3339Nano
// text (matching JSON serialization of the stored data).
func normalizeValue(v any) (any, error) {
	switch x := v.(type) {
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return x, nil
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), nil
	default:
		return nil, fmt.Errorf("%w: value type %T", ember.ErrUnsupportedFilter, v)
	}
}

func normalizeValues(vs []any) ([]any, error) {
	out := make([]any, len(vs))
	for i, v := range vs {
		nv, err := normalizeValue(v)
		if err != nil {
			return nil, err
		}
		out[i] = nv
	}
	return out, nil
}
