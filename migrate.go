package ember

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Migration upgrades a raw entity payload from revision i to i+1.
type Migration func(data []byte) ([]byte, error)

// ErrRevisionAhead is returned when persisted data carries a higher revision
// than the code has migrations for (code rolled back behind the data).
var ErrRevisionAhead = errors.New("ember: stored revision ahead of code")

// Migrate applies ms[stored:] to data, where stored is the revision encoded in
// data (absent -> 0). Migrations run on raw bytes before any struct decode.
func Migrate(data []byte, ms []Migration) ([]byte, error) {
	rev, err := revision(data)
	if err != nil {
		return nil, fmt.Errorf("ember: read revision: %w", err)
	}
	if rev < 0 {
		return nil, fmt.Errorf("ember: negative revision %d", rev)
	}
	if rev > len(ms) {
		return nil, ErrRevisionAhead
	}
	for i := rev; i < len(ms); i++ {
		if data, err = ms[i](data); err != nil {
			return nil, fmt.Errorf("ember: migration %d->%d: %w", i, i+1, err)
		}
	}
	return data, nil
}

func revision(data []byte) (int, error) {
	var pl struct {
		Revision int `json:"revision"`
	}

	if err := json.Unmarshal(data, &pl); err != nil {
		return 0, err
	}
	return pl.Revision, nil
}
