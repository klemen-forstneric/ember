package ember

import "errors"

// Direction
type Direction int

const (
	Ascending Direction = iota
	Descending
)

// Ordering
type Ordering int

const (
	Lexical Ordering = iota
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
