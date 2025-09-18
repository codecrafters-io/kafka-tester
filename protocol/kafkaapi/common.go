package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type FieldEncodable interface {
	Encode(*field_encoder.FieldEncoder)
}

func encodeCompactArray(name string, encoder *field_encoder.FieldEncoder, encodableArray []FieldEncodable) {
	encoder.PushPathContext(name)
	defer encoder.PopPathContext()

	encoder.WriteCompactArrayLengthField("Length", value.NewCompactArrayLength(encodableArray))
	for i, encodable := range encodableArray {
		encoder.PushPathContext(fmt.Sprintf("%s[%d]", name, i))
		encodable.Encode(encoder)
		encoder.PopPathContext()
	}
}
