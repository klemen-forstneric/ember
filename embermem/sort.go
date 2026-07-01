package embermem

import (
	"sort"
	"strconv"
	"time"

	"github.com/klemen-forstneric/ember"
)

// applySort orders items by the sort path using LEXICAL (text) comparison, to
// mirror the SQL backend which extracts a jsonb field as text for ORDER BY
// (no cast). Sort is thus defined for lexically-orderable fields (strings, e.g.
// RFC3339 timestamps); ordering of numeric fields is not guaranteed to match
// across backends. Missing-path placement (here: last) is backend-defined.
func applySort(items []*ember.MarshaledEntity, s ember.Sort) {
	if s.Path == "" {
		return
	}
	sort.SliceStable(items, func(i, j int) bool {
		vi, oki, _ := lookup(items[i], s.Path)
		vj, okj, _ := lookup(items[j], s.Path)
		if !oki || !okj { // missing paths sort last
			return oki && !okj
		}
		ti, tj := textOf(vi), textOf(vj)
		if s.Direction == ember.Descending {
			return ti > tj
		}
		return ti < tj
	})
}

// textOf renders a looked-up value to the text form used for lexical ordering,
// matching how the SQL backend extracts a jsonb field as text.
func textOf(v any) string {
	if s, ok := toStr(v); ok {
		return s
	}
	if f, ok := toFloat(v); ok {
		return strconvFloat(f)
	}
	if b, ok := v.(bool); ok {
		if b {
			return "true"
		}
		return "false"
	}
	return ""
}

// equalJSON compares a looked-up JSON value against a filter operand, coercing
// numbers to float64 and time.Time to its RFC3339Nano string (mirroring how the
// entity serializer stores values).
func equalJSON(got, want any) bool {
	return valueKey(got) == valueKey(want)
}

// orderJSON returns -1/0/1 for got vs want when both are the same ordered kind
// (number or string); ok=false otherwise.
func orderJSON(a, b any) (int, bool) {
	if fa, ok := toFloat(a); ok {
		if fb, ok := toFloat(b); ok {
			switch {
			case fa < fb:
				return -1, true
			case fa > fb:
				return 1, true
			default:
				return 0, true
			}
		}
		return 0, false
	}
	sa, oka := toStr(a)
	sb, okb := toStr(b)
	if oka && okb {
		switch {
		case sa < sb:
			return -1, true
		case sa > sb:
			return 1, true
		default:
			return 0, true
		}
	}
	return 0, false
}

func valueKey(v any) string {
	if f, ok := toFloat(v); ok {
		return "n:" + strconvFloat(f)
	}
	if s, ok := toStr(v); ok {
		return "s:" + s
	}
	if b, ok := v.(bool); ok {
		if b {
			return "b:true"
		}
		return "b:false"
	}
	return "?"
}

func strconvFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	return 0, false
}

func toStr(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), true
	}
	return "", false
}
