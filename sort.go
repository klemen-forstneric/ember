package ember

import "errors"

// Direction is a sort order.
type Direction int

const (
	Ascending Direction = iota
	Descending
)

// Sort orders a List by a single entity path. The zero value (empty Path) means
// unordered. Path uses the same semantics as Filter: reserved paths (id, type,
// version) address top-level storage; any other path addresses a field of the
// entity's data document.
type Sort struct {
	Path      string
	Direction Direction
}

// Unsorted expresses that a List has no ordering requirement. Prefer it over a
// bare Sort{} at call sites so the intent ("order does not matter") is explicit.
func Unsorted() Sort { return Sort{} }

// Asc orders ascending by path.
func Asc(path string) Sort { return Sort{Path: path, Direction: Ascending} }

// Desc orders descending by path.
func Desc(path string) Sort { return Sort{Path: path, Direction: Descending} }

// ErrUnsupportedSort is returned by a backend that cannot order by an arbitrary
// path server-side (e.g. DynamoDB).
var ErrUnsupportedSort = errors.New("ember: unsupported sort")
