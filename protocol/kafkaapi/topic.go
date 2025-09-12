package kafkaapi

import "github.com/codecrafters-io/kafka-tester/protocol/encoder"

type Topic struct {
	UUID       string
	Partitions []Partition
}

func (t Topic) Encode(pe *encoder.Encoder) {
	pe.WriteUUID(t.UUID)
	pe.WriteCompactArrayLength(len(t.Partitions))

	for _, partition := range t.Partitions {
		partition.Encode(pe)
	}

	pe.WriteEmptyTagBuffer()
}
