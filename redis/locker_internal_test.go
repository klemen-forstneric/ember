package redis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewLockerNonPositiveTTLDefaults(t *testing.T) {
	require.Equal(t, defaultLockTTL, NewLocker(nil, 0).ttl)
	require.Equal(t, defaultLockTTL, NewLocker(nil, -time.Second).ttl)
}
