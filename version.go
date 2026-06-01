package ember

// Version
type Version struct {
	initial uint64
	delta   uint64
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
