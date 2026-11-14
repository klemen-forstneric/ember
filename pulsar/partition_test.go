package pulsar

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type PartitionSuite struct {
	suite.Suite
}

func TestPartitionSuite(t *testing.T) {
	suite.Run(t, new(PartitionSuite))
}

func (s *PartitionSuite) TestNamesThePartitionOfTheOrderingKey() {
	s.Equal("persistent://public/default/chat.ws-partition-171", Partition("persistent://public/default/chat.ws", "hello", 512))
}

func (s *PartitionSuite) TestSpreadsKeysOverEveryPartition() {
	counts := map[string]int{}
	for i := range 6400 {
		counts[Partition("chat.ws", fmt.Sprintf("chat-%d", i), 64)]++
	}
	s.Len(counts, 64)
	for p, n := range counts {
		s.Greater(n, 50, p)
	}
}

func (s *PartitionSuite) TestTopicWithoutPartitionsIsItself() {
	s.Equal("chat.ws", Partition("chat.ws", "hello", 0))
}
