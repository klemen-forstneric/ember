package pulsar

import (
	"fmt"
	"hash/fnv"

	"github.com/apache/pulsar-client-go/pulsar"
)

// Partition is the topic of the partition that the producers of a ProducerRegistry send orderingKey to.
func Partition(topic, orderingKey string, partitions int) string {
	if partitions < 1 {
		return topic
	}
	return fmt.Sprintf("%s-partition-%d", topic, partitionOf(orderingKey, uint32(partitions)))
}

// route is the producers' MessageRouter: a message goes to the partition of its key, the event's ordering key.
func route(msg *pulsar.ProducerMessage, topic pulsar.TopicMetadata) int {
	return partitionOf(msg.Key, topic.NumPartitions())
}

func partitionOf(key string, partitions uint32) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % partitions)
}
