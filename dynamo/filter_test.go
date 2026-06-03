package dynamo

import (
	"github.com/klemen-forstneric/ember"
)

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)
