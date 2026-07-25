package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"

	emberredis "github.com/klemen-forstneric/ember/redis"
)

type LockerSuite struct {
	suite.Suite
	mr     *miniredis.Miniredis
	locker *emberredis.Locker
}

func TestLockerSuite(t *testing.T) {
	suite.Run(t, new(LockerSuite))
}

func (s *LockerSuite) SetupTest() {
	mr, err := miniredis.Run()
	s.Require().NoError(err)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	s.T().Cleanup(func() {
		_ = client.Close()
		mr.Close()
	})
	s.mr = mr
	s.locker = emberredis.NewLocker(client, time.Minute)
}

func (s *LockerSuite) TestAcquireThenReleaseFrees() {
	ctx := context.Background()

	lock, err := s.locker.TryLock(ctx, "k")
	s.Require().NoError(err)
	s.Require().NotNil(lock)

	s.Require().NoError(lock.Release(ctx))

	// After release the key is free — a second acquire succeeds.
	lock2, err := s.locker.TryLock(ctx, "k")
	s.Require().NoError(err)
	s.NotNil(lock2)
}

func (s *LockerSuite) TestStaleReleaseDoesNotDeleteNewHolder() {
	ctx := context.Background()

	// Holder A acquires.
	lockA, err := s.locker.TryLock(ctx, "k")
	s.Require().NoError(err)
	s.Require().NotNil(lockA)

	// A's lock expires and holder B acquires the same key.
	s.mr.FastForward(2 * time.Minute)
	lockB, err := s.locker.TryLock(ctx, "k")
	s.Require().NoError(err)
	s.Require().NotNil(lockB)

	// A finishes late and releases — must NOT delete B's lock.
	s.Require().NoError(lockA.Release(ctx))

	// B still holds it: a fresh acquire fails (returns nil).
	lockC, err := s.locker.TryLock(ctx, "k")
	s.Require().NoError(err)
	s.Nil(lockC, "B's lock must survive A's stale release")
}
