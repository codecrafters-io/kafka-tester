package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
)

type FieldEncodable interface {
	Encode(*field_encoder.FieldEncoder)
}

func encodeCompactArray(name string, encoder *field_encoder.FieldEncoder, encodableArray []FieldEncodable) {
	encoder.PushPathContext(name)
	encoder.PopPathContext()

	encoder.WriteCompactArrayLengthField("Length", len(encodableArray))
	for _, encodable := range encodableArray {
		encodable.Encode(encoder)
	}
}
