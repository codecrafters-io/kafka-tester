package kafkaapi

import "github.com/codecrafters-io/kafka-tester/protocol/encoder"

type Topic struct {
	TopicUUID  string
	Partitions []Partition
}

func (t Topic) Encode(pe *encoder.Encoder) {
	pe.WriteUUID(t.TopicUUID)
	pe.WriteCompactArrayLength(len(t.Partitions))

	for _, partition := range t.Partitions {
		partition.Encode(pe)
	}

	pe.WriteEmptyTagBuffer()
}
