package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultRelayConfigDerivesNamesFromService(t *testing.T) {
	cfg := DefaultRelayConfig("order")

	require.Equal(t, "order_events_slot", cfg.SlotName)
	require.Equal(t, "order_events_pub", cfg.PublicationName)
	require.Equal(t, "order_events", cfg.MessagePrefix)
	require.NoError(t, validateRelayConfig(cfg))
}

func TestValidateRelayConfigRejectsBadValues(t *testing.T) {
	cases := map[string]func(*RelayConfig){
		"empty slot name":        func(c *RelayConfig) { c.SlotName = "" },
		"empty publication name": func(c *RelayConfig) { c.PublicationName = "" },
		"empty message prefix":   func(c *RelayConfig) { c.MessagePrefix = "" },
		"zero keepalive":         func(c *RelayConfig) { c.KeepAliveInterval = 0 },
		"zero acquire interval":  func(c *RelayConfig) { c.AcquireInterval = 0 },
		"zero max backoff":       func(c *RelayConfig) { c.MaxRetryBackoff = 0 },
		"negative keepalive":     func(c *RelayConfig) { c.KeepAliveInterval = -time.Second },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultRelayConfig("order")
			mutate(&cfg)
			require.ErrorIs(t, validateRelayConfig(cfg), ErrInvalidRelayConfig)
		})
	}
}
