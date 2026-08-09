package ember

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

var (
	ErrInvalidPage   = errors.New("ember: invalid page")
	ErrInvalidCursor = errors.New("ember: invalid cursor")
)

// Cursor
type Cursor struct {
	Value any
	ID    string
}

func (c Cursor) IsZero() bool { return c.ID == "" }

// token
type token struct {
	Kind  string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
	ID    string `json:"i"`
}

func (c Cursor) MarshalText() ([]byte, error) {
	if c.ID == "" {
		if c.Value != nil {
			return nil, fmt.Errorf("%w: value without id", ErrInvalidCursor)
		}
		return nil, nil
	}

	w := token{ID: c.ID}
	switch x := c.Value.(type) {
	case nil:
	case string:
		w.Kind, w.Value = "s", x
	case time.Time:
		w.Kind, w.Value = "s", x.UTC().Format(time.RFC3339Nano)
	case bool:
		w.Kind, w.Value = "b", strconv.FormatBool(x)
	case uint:
		if uint64(x) > math.MaxInt64 {
			return nil, fmt.Errorf("%w: value %d exceeds int64", ErrInvalidCursor, x)
		}
		w.Kind, w.Value = "n", strconv.FormatUint(uint64(x), 10)
	case uint64:
		if x > math.MaxInt64 {
			return nil, fmt.Errorf("%w: value %d exceeds int64", ErrInvalidCursor, x)
		}
		w.Kind, w.Value = "n", strconv.FormatUint(x, 10)
	case int, int8, int16, int32, int64, uint8, uint16, uint32:
		w.Kind, w.Value = "n", fmt.Sprintf("%d", x)
	case float32, float64:
		w.Kind, w.Value = "f", fmt.Sprintf("%v", x)
	default:
		return nil, fmt.Errorf("%w: value type %T", ErrInvalidCursor, c.Value)
	}

	raw, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}

	return []byte(base64.RawURLEncoding.EncodeToString(raw)), nil
}

func (c *Cursor) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		*c = Cursor{}
		return nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(string(b))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}

	var w token
	if err := json.Unmarshal(raw, &w); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	if w.ID == "" {
		return fmt.Errorf("%w: missing id", ErrInvalidCursor)
	}

	switch w.Kind {
	case "":
		c.Value = nil
	case "s":
		c.Value = w.Value
	case "b":
		b, err := strconv.ParseBool(w.Value)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidCursor, err)
		}
		c.Value = b
	case "n":
		i, err := strconv.ParseInt(w.Value, 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidCursor, err)
		}
		c.Value = i
	case "f":
		f, err := strconv.ParseFloat(w.Value, 64)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidCursor, err)
		}
		c.Value = f
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidCursor, w.Kind)
	}

	c.ID = w.ID

	return nil
}

// Page
type Page struct {
	Limit  int
	Offset int
	Cursor Cursor
}

func Unpaged() Page { return Page{} }

func Limit(n int) Page { return Page{Limit: n} }

func (p Page) Skip(n int) Page {
	p.Offset = n
	return p
}

func (p Page) After(value any, id string) Page {
	p.Cursor = Cursor{Value: value, ID: id}
	return p
}

func (p Page) AfterCursor(c Cursor) Page {
	p.Cursor = c
	return p
}

func (p Page) IsZero() bool {
	return p.Limit == 0 && p.Offset == 0 && p.Cursor.IsZero()
}

func (p Page) Validate() error {
	if p.Limit < 0 {
		return fmt.Errorf("%w: negative limit %d", ErrInvalidPage, p.Limit)
	}
	if p.Offset < 0 {
		return fmt.Errorf("%w: negative offset %d", ErrInvalidPage, p.Offset)
	}
	if p.Offset > 0 && !p.Cursor.IsZero() {
		return fmt.Errorf("%w: offset and cursor are mutually exclusive", ErrInvalidPage)
	}
	if p.Limit == 0 && (p.Offset > 0 || !p.Cursor.IsZero()) {
		return fmt.Errorf("%w: offset or cursor requires a limit", ErrInvalidPage)
	}
	if p.Cursor.ID == "" && p.Cursor.Value != nil {
		return fmt.Errorf("%w: cursor value without id", ErrInvalidPage)
	}
	return nil
}
