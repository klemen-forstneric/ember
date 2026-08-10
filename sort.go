package ember

// Direction
type Direction int

const (
	DirectionAscending Direction = iota
	DirectionDescending
)

// Ordering
type Ordering int

const (
	orderingUndeclared Ordering = iota
	OrderingLexical
	OrderingNumeric
)

// Sort
type Sort struct {
	Path      string
	Direction Direction
	Ordering  Ordering
}

func Unsorted() Sort { return Sort{} }

func AscLex(path string) Sort {
	return Sort{Path: path, Direction: DirectionAscending, Ordering: OrderingLexical}
}

func AscNum(path string) Sort {
	return Sort{Path: path, Direction: DirectionAscending, Ordering: OrderingNumeric}
}

func DescLex(path string) Sort {
	return Sort{Path: path, Direction: DirectionDescending, Ordering: OrderingLexical}
}

func DescNum(path string) Sort {
	return Sort{Path: path, Direction: DirectionDescending, Ordering: OrderingNumeric}
}
