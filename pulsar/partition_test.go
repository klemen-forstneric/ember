package pulsar

import (
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
	s.Equal("persistent://public/default/chat.ws-partition-210", Partition("persistent://public/default/chat.ws", "hello", 512))
}

// The key's Java hashCode is negative. The Go client keeps all 32 bits; the Java client would pick partition 0.
func (s *PartitionSuite) TestHashesAsTheGoClientDoes() {
	s.Equal("chat.ws-partition-8", Partition("chat.ws", "polygenelubricants", 12))
}

func (s *PartitionSuite) TestTopicWithoutPartitionsIsItself() {
	s.Equal("chat.ws", Partition("chat.ws", "hello", 0))
}
