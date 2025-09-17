package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
)

type Topic struct {
	UUID       string
	Partitions []Partition
}

func (t Topic) Encode(encoder *field_encoder.FieldEncoder) {
	encoder.WriteUUIDField("UUID", t.UUID)
	t.encodePartitions(encoder)
	encoder.WriteEmptyTagBuffer()
}

func (t Topic) encodePartitions(encoder *field_encoder.FieldEncoder) {
	encodablePartitions := make([]FieldEncodable, len(t.Partitions))
	for i, topic := range t.Partitions {
		encodablePartitions[i] = topic
	}
	encodeCompactArray("ForgottenTopics", encoder, encodablePartitions)
}
