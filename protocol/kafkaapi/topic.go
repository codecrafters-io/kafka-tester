package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
)

type Topic struct {
	UUID       string
	Partitions []Partition
}

func (t Topic) Encode(encoder *field_encoder.FieldEncoder) {
	encoder.WriteUUID("UUID", t.UUID)
	t.encodePartitions(encoder)
	encoder.WriteEmptyTagBuffer()
}

func (t Topic) encodePartitions(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Partitions")
	defer encoder.PopPathContext()

	encoder.WriteCompactArrayLengthField("Length", len(t.Partitions))
	for _, partition := range t.Partitions {
		partition.Encode(encoder)
	}
}
