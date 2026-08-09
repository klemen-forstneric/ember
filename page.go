package ember

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var (
	ErrInvalidPage     = errors.New("ember: invalid page")
	ErrInvalidCursor   = errors.New("ember: invalid cursor")
	ErrUnsupportedPage = errors.New("ember: unsupported page")
)

// Cursor
type Cursor struct {
	Value any
	ID    string
}

func (c Cursor) IsZero() bool { return c.ID == "" }

// cursorWire
type cursorWire struct {
	Kind  string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
	ID    string `json:"i"`
}

func (c Cursor) MarshalText() ([]byte, error) {
	w := cursorWire{ID: c.ID}
	switch x := c.Value.(type) {
	case nil:
	case string:
		w.Kind, w.Value = "s", x
	case time.Time:
		w.Kind, w.Value = "s", x.UTC().Format(time.RFC3339Nano)
	case bool:
		w.Kind, w.Value = "s", strconv.FormatBool(x)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		w.Kind, w.Value = "n", fmt.Sprintf("%d", x)
	case float32, float64:
		w.Kind, w.Value = "n", fmt.Sprintf("%v", x)
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
	raw, err := base64.RawURLEncoding.DecodeString(string(b))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}

	var w cursorWire
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
	case "n":
		if i, err := strconv.ParseInt(w.Value, 10, 64); err == nil {
			c.Value = i
			break
		}
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
	return nil
}
