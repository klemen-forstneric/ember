package wal

import (
	"errors"
	"fmt"
	"time"
)

// RelayConfig
type RelayConfig struct {
	SlotName          string
	PublicationName   string
	MessagePrefix     string
	KeepAliveInterval time.Duration // standby update cadence
	AcquireInterval   time.Duration // slot contention retry, jittered
	MaxRetryBackoff   time.Duration // cap on publish retry backoff
}

const (
	defaultKeepAliveInterval = 10 * time.Second
	defaultAcquireInterval   = 5 * time.Second
	defaultMaxRetryBackoff   = 30 * time.Second
)

// DefaultRelayConfig derives slot, publication and prefix from the service
// name. The prefix must match the one given to NewEventRepository.
func DefaultRelayConfig(service string) RelayConfig {
	return RelayConfig{
		SlotName:          service + "_events_slot",
		PublicationName:   service + "_events_pub",
		MessagePrefix:     service + "_events",
		KeepAliveInterval: defaultKeepAliveInterval,
		AcquireInterval:   defaultAcquireInterval,
		MaxRetryBackoff:   defaultMaxRetryBackoff,
	}
}

// ErrInvalidRelayConfig is returned by NewRelay when cfg fails validation.
var ErrInvalidRelayConfig = errors.New("ember/wal: invalid relay config")

func validateRelayConfig(cfg RelayConfig) error {
	switch {
	case cfg.SlotName == "":
		return fmt.Errorf("%w: SlotName must not be empty", ErrInvalidRelayConfig)
	case cfg.PublicationName == "":
		return fmt.Errorf("%w: PublicationName must not be empty", ErrInvalidRelayConfig)
	case cfg.MessagePrefix == "":
		return fmt.Errorf("%w: MessagePrefix must not be empty", ErrInvalidRelayConfig)
	case cfg.KeepAliveInterval <= 0:
		return fmt.Errorf("%w: KeepAliveInterval must be positive", ErrInvalidRelayConfig)
	case cfg.AcquireInterval <= 0:
		return fmt.Errorf("%w: AcquireInterval must be positive", ErrInvalidRelayConfig)
	case cfg.MaxRetryBackoff <= 0:
		return fmt.Errorf("%w: MaxRetryBackoff must be positive", ErrInvalidRelayConfig)
	}
	return nil
}
