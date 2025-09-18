package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Topic struct {
	UUID       value.UUID
	Partitions []Partition
}

func (t Topic) Encode(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Topic")
	defer encoder.PopPathContext()
	encoder.WriteUUIDField("UUID", t.UUID)
	t.encodePartitions(encoder)
	encoder.WriteEmptyTagBuffer()
}

func (t Topic) encodePartitions(encoder *field_encoder.FieldEncoder) {
	encodablePartitions := make([]FieldEncodable, len(t.Partitions))
	for i, topic := range t.Partitions {
		encodablePartitions[i] = topic
	}
	encodeCompactArray("Partitions", encoder, encodablePartitions)
}
