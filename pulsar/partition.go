package pulsar

import "fmt"

// Partition is the topic of the partition that the producers of a ProducerRegistry send orderingKey to.
func Partition(topic, orderingKey string, partitions int) string {
	if partitions < 1 {
		return topic
	}
	// pulsar.JavaStringHash, which the client does not export.
	var h uint32
	for i := range len(orderingKey) {
		h = 31*h + uint32(orderingKey[i])
	}
	return fmt.Sprintf("%s-partition-%d", topic, h%uint32(partitions))
}
