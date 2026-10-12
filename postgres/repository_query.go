package postgres

import (
	"fmt"
	"reflect"

	sq "github.com/Masterminds/squirrel"
	"github.com/klemen-forstneric/ember"
)

func (r *Repository[E]) column(path string) (string, error) {
	col, ok := r.paths[path]
	if !ok {
		return "", fmt.Errorf("%w: unknown column %q", ember.ErrUnsupportedFilter, path)
	}
	return col, nil
}

func queryValue(value any) (any, error) {
	if value == nil || !scalarType(reflect.TypeOf(value)) {
		return nil, fmt.Errorf("%w: value type %T", ember.ErrUnsupportedFilter, value)
	}
	return nativeValue(reflect.ValueOf(value)), nil
}

func (r *Repository[E]) predicate(filter ember.Filter) (sq.Sqlizer, error) {
	if filter == nil {
		return sq.Expr("TRUE"), nil
	}
	switch f := filter.(type) {
	case ember.Comparison:
		col, err := r.column(f.Path)
		if err != nil {
			return nil, err
		}
		v, err := queryValue(f.Value)
		if err != nil {
			return nil, err
		}
		p, err := compare(col, f.Op, v)
		if err != nil {
			return nil, err
		}
		return sq.And{sq.Expr(col + " IS NOT NULL"), p}, nil
	case ember.Membership:
		col, err := r.column(f.Path)
		if err != nil {
			return nil, err
		}
		if len(f.Values) == 0 {
			return sq.Expr("FALSE"), nil
		}
		values := make([]any, len(f.Values))
		for i, v := range f.Values {
			values[i], err = queryValue(v)
			if err != nil {
				return nil, err
			}
		}
		return sq.And{sq.Expr(col + " IS NOT NULL"), sq.Eq{col: values}}, nil
	case ember.Existence:
		col, err := r.column(f.Path)
		if err != nil {
			return nil, err
		}
		if f.Exists {
			return sq.Expr(col + " IS NOT NULL"), nil
		}
		return sq.Expr(col + " IS NULL"), nil
	case ember.Negation:
		p, err := r.predicate(f.Filter)
		if err != nil {
			return nil, err
		}
		return sq.Expr("NOT (?)", p), nil
	case ember.Conjunction:
		ps, err := r.predicates(f.Filters)
		if err != nil {
			return nil, err
		}
		return sq.And(ps), nil
	case ember.Disjunction:
		ps, err := r.predicates(f.Filters)
		if err != nil {
			return nil, err
		}
		return sq.Or(ps), nil
	default:
		return nil, fmt.Errorf("%w: unknown node %T", ember.ErrUnsupportedFilter, filter)
	}
}

func (r *Repository[E]) predicates(filters []ember.Filter) ([]sq.Sqlizer, error) {
	ps := make([]sq.Sqlizer, 0, len(filters))
	for _, f := range filters {
		p, err := r.predicate(f)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	return ps, nil
}

// Native column types determine ordering; no column casts or JSON extraction
// appear in predicates, ordering, or seeks. This allows ordinary B-tree indexes.
func (r *Repository[E]) listQuery(filter ember.Filter, sort ember.Sort, page ember.Page) (string, []any, error) {
	if err := page.Validate(); err != nil {
		return "", nil, err
	}
	p, err := r.predicate(filter)
	if err != nil {
		return "", nil, err
	}
	qb := psql.Select(r.columns...).From(r.table).Where(p)
	col := `"id"`
	dir, op := "ASC", ">"
	if sort.Path != "" {
		col, err = r.column(sort.Path)
		if err != nil {
			return "", nil, err
		}
		if sort.Direction == ember.DirectionDescending {
			dir, op = "DESC", "<"
		}
	}
	if sort.Path != "" || !page.IsZero() {
		qb = qb.OrderBy(col + " " + dir)
		if col != `"id"` {
			qb = qb.OrderBy(`"id" ` + dir)
		}
	}
	if !page.Cursor.IsZero() {
		var seek sq.Sqlizer
		switch {
		case col == `"id"`:
			seek = sq.Expr(`"id" `+op+` ?`, page.Cursor.ID)
		case page.Cursor.Value == nil:
			if !r.nullable[sort.Path] {
				return "", nil, fmt.Errorf("%w: nonnullable sort requires a cursor value", ember.ErrInvalidCursor)
			}
			seek = sq.And{sq.Expr(col + " IS NULL"), sq.Expr(`"id" `+op+` ?`, page.Cursor.ID)}
			if dir == "DESC" {
				seek = sq.Or{seek, sq.Expr(col + " IS NOT NULL")}
			}
		default:
			value, err := queryValue(page.Cursor.Value)
			if err != nil {
				return "", nil, fmt.Errorf("%w: %v", ember.ErrInvalidCursor, err)
			}
			seek = sq.Expr("("+col+`, "id") `+op+" (?, ?)", value, page.Cursor.ID)
			if dir == "ASC" && r.nullable[sort.Path] {
				seek = sq.Or{seek, sq.Expr(col + " IS NULL")}
			}
		}
		qb = qb.Where(seek)
	}
	if page.Limit > 0 {
		qb = qb.Limit(uint64(page.Limit))
	}
	if page.Offset > 0 {
		qb = qb.Offset(uint64(page.Offset))
	}
	return qb.ToSql()
}
