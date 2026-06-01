package ember

import "errors"

// ErrUnsupportedFilter is returned by a repository when it cannot translate a
// filter node, operator, or value type into its native query dialect.
var ErrUnsupportedFilter = errors.New("ember: unsupported filter")

// Operator is a comparison operator used by Comparison.
type Operator int

const (
	OpEq Operator = iota
	OpNe
	OpGt
	OpGte
	OpLt
	OpLte
)

// Filter is a sealed sum type. Only ember-defined nodes satisfy it, so
// repositories can translate the closed set exhaustively.
//
// Null/missing-path semantics (two-valued): a path predicate (Comparison,
// Membership) is satisfied only when the referenced path is present, non-null,
// and the comparison holds; a missing or null path makes that leaf false.
// And/Or/Not combine leaves as ordinary booleans. As a result Ne(p, x) does not
// match entities where p is absent/null, while Not(Eq(p, x)) does. Exists(p, true)
// means present and non-null; Exists(p, false) is its complement. Every backend
// honors these semantics identically.
type Filter interface {
	isFilter()
}

// Comparison matches a single path against a value with an operator.
type Comparison struct {
	Path  string
	Op    Operator
	Value any
}

// Membership matches when the path's value is one of Values (IN).
type Membership struct {
	Path   string
	Values []any
}

// Existence matches on whether the path is present (and non-null).
type Existence struct {
	Path   string
	Exists bool
}

// Conjunction is a logical AND over its children.
type Conjunction struct {
	Filters []Filter
}

// Disjunction is a logical OR over its children.
type Disjunction struct {
	Filters []Filter
}

// Negation is a logical NOT of its child.
type Negation struct {
	Filter Filter
}

func (Comparison) isFilter()  {}
func (Membership) isFilter()  {}
func (Existence) isFilter()   {}
func (Conjunction) isFilter() {}
func (Disjunction) isFilter() {}
func (Negation) isFilter()    {}

// Eq matches path == v.
func Eq(path string, v any) Filter { return Comparison{Path: path, Op: OpEq, Value: v} }

// Ne matches path != v.
func Ne(path string, v any) Filter { return Comparison{Path: path, Op: OpNe, Value: v} }

// Gt matches path > v.
func Gt(path string, v any) Filter { return Comparison{Path: path, Op: OpGt, Value: v} }

// Gte matches path >= v.
func Gte(path string, v any) Filter { return Comparison{Path: path, Op: OpGte, Value: v} }

// Lt matches path < v.
func Lt(path string, v any) Filter { return Comparison{Path: path, Op: OpLt, Value: v} }

// Lte matches path <= v.
func Lte(path string, v any) Filter { return Comparison{Path: path, Op: OpLte, Value: v} }

// In matches when the path's value is one of vs.
func In(path string, vs ...any) Filter { return Membership{Path: path, Values: vs} }

// Exists matches on whether the path is present (exists=true) or absent (exists=false).
func Exists(path string, exists bool) Filter { return Existence{Path: path, Exists: exists} }

// And combines filters with logical AND.
func And(fs ...Filter) Filter { return Conjunction{Filters: fs} }

// Or combines filters with logical OR.
func Or(fs ...Filter) Filter { return Disjunction{Filters: fs} }

// Not negates a filter.
func Not(f Filter) Filter { return Negation{Filter: f} }
