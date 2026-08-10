package ember

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

func AscLex(path string) Sort {
	return Sort{Path: path, Direction: Ascending, Ordering: Lexical}
}

func AscNum(path string) Sort {
	return Sort{Path: path, Direction: Ascending, Ordering: Numeric}
}

func DescLex(path string) Sort {
	return Sort{Path: path, Direction: Descending, Ordering: Lexical}
}

func DescNum(path string) Sort {
	return Sort{Path: path, Direction: Descending, Ordering: Numeric}
}
