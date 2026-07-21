package ember

import "encoding/json"

// Version
type Version struct {
	initial uint64
	delta   uint64
}

// MarshalJSON emits the resolved value; a decoded Version has delta 0, matching
// how the store collapses a persisted version on read.
func (v Version) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.Value())
}

func (v *Version) UnmarshalJSON(b []byte) error {
	var n uint64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*v = Version{initial: n}
	return nil
}

func NewVersion(initial uint64) Version {
	return Version{initial: initial}
}

func (v Version) Inc() Version {
	return Version{initial: v.initial, delta: v.delta + 1}
}

func (v Version) Initial() uint64 {
	return v.initial
}

func (v Version) Value() uint64 {
	return v.initial + v.delta
}
