package postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/klemen-forstneric/ember"
)

// buildWhere translates a Filter into a SQL boolean expression with bind
// arguments. Placeholders are numbered from $1. A nil filter yields an empty
// expression (match all).
func buildWhere(f ember.Filter) (string, []any, error) {
	if f == nil {
		return "", nil, nil
	}
	t := &translator{}
	expr, err := t.node(f)
	if err != nil {
		return "", nil, err
	}
	return expr, t.args, nil
}

type translator struct {
	args []any
}

func (t *translator) placeholder(v any) (string, error) {
	nv, err := normalizeValue(v)
	if err != nil {
		return "", err
	}
	t.args = append(t.args, nv)
	return fmt.Sprintf("$%d", len(t.args)), nil
}

func (t *translator) node(f ember.Filter) (string, error) {
	switch n := f.(type) {
	case ember.Comparison:
		return t.comparison(n)
	case ember.Membership:
		return t.membership(n)
	case ember.Existence:
		return existence(n), nil
	case ember.Conjunction:
		return t.composite(n.Filters, "AND", "TRUE")
	case ember.Disjunction:
		return t.composite(n.Filters, "OR", "FALSE")
	case ember.Negation:
		inner, err := t.node(n.Filter)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("NOT (%s)", inner), nil
	default:
		return "", fmt.Errorf("%w: unknown node %T", ember.ErrUnsupportedFilter, f)
	}
}

func (t *translator) comparison(c ember.Comparison) (string, error) {
	col, reserved := column(c.Path)
	op, err := sqlOp(c.Op)
	if err != nil {
		return "", err
	}
	ph, err := t.placeholder(c.Value)
	if err != nil {
		return "", err
	}
	if reserved {
		return fmt.Sprintf("%s %s %s", col, op, ph), nil
	}
	// Guard a jsonb path so a missing/null value makes the leaf strictly false
	// (two-valued semantics) instead of SQL NULL. The guard checks the uncast
	// text extraction; the comparison uses the (possibly cast) expression.
	return fmt.Sprintf("%s IS NOT NULL AND %s %s %s", col, castFor(col, c.Value), op, ph), nil
}

func (t *translator) membership(m ember.Membership) (string, error) {
	if len(m.Values) == 0 {
		return "FALSE", nil
	}
	col, reserved := column(m.Path)
	phs := make([]string, 0, len(m.Values))
	for _, v := range m.Values {
		ph, err := t.placeholder(v)
		if err != nil {
			return "", err
		}
		phs = append(phs, ph)
	}
	in := strings.Join(phs, ", ")
	if reserved {
		return fmt.Sprintf("%s IN (%s)", col, in), nil
	}
	return fmt.Sprintf("%s IS NOT NULL AND %s IN (%s)", col, castFor(col, m.Values[0]), in), nil
}

func (t *translator) composite(fs []ember.Filter, joiner, empty string) (string, error) {
	if len(fs) == 0 {
		return empty, nil
	}
	parts := make([]string, 0, len(fs))
	for _, f := range fs {
		expr, err := t.node(f)
		if err != nil {
			return "", err
		}
		parts = append(parts, "("+expr+")")
	}
	return strings.Join(parts, " "+joiner+" "), nil
}

func existence(e ember.Existence) string {
	col, _ := column(e.Path)
	if e.Exists {
		return col + " IS NOT NULL"
	}
	return col + " IS NULL"
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

func sqlOp(op ember.Operator) (string, error) {
	switch op {
	case ember.OpEq:
		return "=", nil
	case ember.OpNe:
		return "<>", nil
	case ember.OpGt:
		return ">", nil
	case ember.OpGte:
		return ">=", nil
	case ember.OpLt:
		return "<", nil
	case ember.OpLte:
		return "<=", nil
	default:
		return "", fmt.Errorf("%w: operator %d", ember.ErrUnsupportedFilter, op)
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
