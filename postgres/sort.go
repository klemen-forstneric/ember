package postgres

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/klemen-forstneric/ember"
)

func sortExpr(s ember.Sort) string {
	col, reserved := column(s.Path)
	if reserved || s.Ordering != ember.OrderingNumeric {
		return col
	}
	return "(" + col + ")::numeric"
}

func orderBy(s ember.Sort, paged bool) []string {
	dir := direction(s.Direction)

	switch {
	case s.Path == "" && !paged:
		return nil
	case s.Path == "":
		return []string{"id ASC"}
	case !paged:
		return []string{sortExpr(s) + " " + dir}
	default:
		return []string{sortExpr(s) + " " + dir, "id " + dir}
	}
}

func seekPredicate(s ember.Sort, c ember.Cursor) (sq.Sqlizer, error) {
	if s.Path == "" {
		return sq.Expr("id > ?", c.ID), nil
	}

	op := ">"
	if s.Direction == ember.DirectionDescending {
		op = "<"
	}

	if c.Value == nil {
		return nil, fmt.Errorf("%w: sorted cursor requires a value", ember.ErrInvalidCursor)
	}

	v, err := normalizeValue(c.Value)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ember.ErrInvalidCursor, err)
	}

	placeholder := "?"
	if _, reserved := column(s.Path); !reserved && s.Ordering == ember.OrderingNumeric {
		placeholder = "?::numeric"
	}

	return sq.Expr("("+sortExpr(s)+", id) "+op+" ("+placeholder+", ?)", v, c.ID), nil
}

func direction(d ember.Direction) string {
	if d == ember.DirectionDescending {
		return "DESC"
	}
	return "ASC"
}
