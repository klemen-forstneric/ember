package embermem

import (
	"encoding/json"
	"fmt"

	"github.com/klemen-forstneric/ember"
)

// matches reports whether the entity satisfies the filter, honoring ember's
// two-valued semantics: a path predicate is false when the path is absent/null.
func matches(f ember.Filter, m *ember.MarshaledEntity) (bool, error) {
	if f == nil {
		return true, nil
	}
	switch n := f.(type) {
	case ember.Comparison:
		v, ok, err := lookup(m, n.Path)
		if err != nil || !ok {
			return false, err
		}
		return compare(v, n.Op, n.Value), nil
	case ember.Membership:
		v, ok, err := lookup(m, n.Path)
		if err != nil || !ok {
			return false, err
		}
		for _, want := range n.Values {
			if equalJSON(v, want) {
				return true, nil
			}
		}
		return false, nil
	case ember.Existence:
		_, ok, err := lookup(m, n.Path)
		if err != nil {
			return false, err
		}
		return ok == n.Exists, nil
	case ember.Conjunction:
		for _, sub := range n.Filters {
			ok, err := matches(sub, m)
			if err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	case ember.Disjunction:
		for _, sub := range n.Filters {
			ok, err := matches(sub, m)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	case ember.Negation:
		ok, err := matches(n.Filter, m)
		if err != nil {
			return false, err
		}
		return !ok, nil
	default:
		return false, fmt.Errorf("%w: unknown node %T", ember.ErrUnsupportedFilter, f)
	}
}

// lookup resolves a filter path to a value from the entity. Reserved paths read
// top-level fields; others read a dotted path from the data document. The bool
// is false when the path is absent or JSON null.
func lookup(m *ember.MarshaledEntity, path string) (any, bool, error) {
	switch path {
	case "id":
		return m.ID, true, nil
	case "type":
		return m.Type, true, nil
	case "version":
		return float64(m.Version.Value()), true, nil
	}

	var data map[string]any
	if err := json.Unmarshal(m.Data, &data); err != nil {
		return nil, false, err
	}
	cur := any(data)
	for _, seg := range splitPath(path) {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false, nil
		}
		cur, ok = obj[seg]
		if !ok {
			return nil, false, nil
		}
	}
	if cur == nil {
		return nil, false, nil
	}
	return cur, true, nil
}

func splitPath(path string) []string {
	segs := []string{}
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			segs = append(segs, path[start:i])
			start = i + 1
		}
	}
	return append(segs, path[start:])
}

// compare applies the operator between a looked-up value and the filter operand.
// JSON numbers decode to float64; time/strings compare lexically.
func compare(got any, op ember.Operator, want any) bool {
	switch op {
	case ember.OpEq:
		return equalJSON(got, want)
	case ember.OpNe:
		return !equalJSON(got, want)
	}
	// ordered comparisons
	c, ok := orderJSON(got, want)
	if !ok {
		return false
	}
	switch op {
	case ember.OpGt:
		return c > 0
	case ember.OpGte:
		return c >= 0
	case ember.OpLt:
		return c < 0
	case ember.OpLte:
		return c <= 0
	}
	return false
}
