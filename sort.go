package ember

import (
	"errors"
	"fmt"
)

// Direction
type Direction int

const (
	Ascending Direction = iota
	Descending
)

// Ordering
type Ordering int

const (
	orderingUndeclared Ordering = iota
	Lexical
	Numeric
)

// Sort
type Sort struct {
	Path      string
	Direction Direction
	Ordering  Ordering
}

func Unsorted() Sort { return Sort{} }

func Asc(path string) Sort { return Sort{Path: path, Direction: Ascending} }

func Desc(path string) Sort { return Sort{Path: path, Direction: Descending} }

func (s Sort) Lexical() Sort {
	s.Ordering = Lexical
	return s
}

func (s Sort) Numeric() Sort {
	s.Ordering = Numeric
	return s
}

var ErrUnsupportedSort = errors.New("ember: unsupported sort")
var ErrInvalidSort = errors.New("ember: invalid sort")

func (s Sort) Validate() error {
	if s.Path == "" || reservedPath(s.Path) {
		return nil
	}
	if s.Ordering == orderingUndeclared {
		return fmt.Errorf("%w: path %q needs an explicit Lexical() or Numeric() ordering", ErrInvalidSort, s.Path)
	}
	return nil
}

func reservedPath(path string) bool {
	switch path {
	case "id", "type", "version":
		return true
	default:
		return false
	}
}
