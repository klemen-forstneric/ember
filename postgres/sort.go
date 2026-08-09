package postgres

import "github.com/klemen-forstneric/ember"

func sortExpr(s ember.Sort) string {
	col, reserved := column(s.Path)
	if reserved || s.Ordering != ember.Numeric {
		return col
	}
	return "(" + col + ")::numeric"
}

func orderBy(s ember.Sort) []string {
	if s.Path == "" {
		return nil
	}
	return []string{sortExpr(s) + " " + direction(s.Direction)}
}

func direction(d ember.Direction) string {
	if d == ember.Descending {
		return "DESC"
	}
	return "ASC"
}
