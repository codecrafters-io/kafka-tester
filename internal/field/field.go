package field

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Field struct {
	Path  field_path.FieldPath
	Value value.KafkaProtocolValue
}
