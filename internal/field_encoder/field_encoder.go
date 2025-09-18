package field_encoder

import (
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field"
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	kafka_value "github.com/codecrafters-io/kafka-tester/protocol/value"
)

type FieldEncoder struct {
	currentPathContexts []string
	encoder             *encoder.Encoder
	encodedFields       []field.Field
}

func NewFieldEncoder() *FieldEncoder {
	return &FieldEncoder{
		currentPathContexts: []string{},
		encodedFields:       []field.Field{},
		encoder:             encoder.NewEncoder(),
	}
}

func (e *FieldEncoder) EncodedFields() []field.Field {
	return e.encodedFields
}

func (e *FieldEncoder) PushPathContext(pathContext string) {
	e.currentPathContexts = append(e.currentPathContexts, pathContext)
}

func (e *FieldEncoder) PopPathContext() {
	e.currentPathContexts = e.currentPathContexts[:len(e.currentPathContexts)-1]
}

func (e *FieldEncoder) currentPath() field_path.FieldPath {
	return field_path.NewFieldPath(strings.Join(e.currentPathContexts, "."))
}

func (e *FieldEncoder) appendEncodedField(encodedValue kafka_value.KafkaProtocolValue) {
	e.encodedFields = append(e.encodedFields, field.Field{
		Value: encodedValue,
		Path:  e.currentPath(),
	})
}

func (e *FieldEncoder) Bytes() []byte {
	return e.encoder.Bytes()
}

func (e *FieldEncoder) WriteInt16Field(variableName string, value kafka_value.Int16) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteInt16(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteInt32Field(variableName string, value kafka_value.Int32) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteInt32(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteStringField(variableName string, value kafka_value.String) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteString(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteCompactStringField(variableName string, value kafka_value.CompactString) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteCompactString(value.Value)
	e.appendEncodedField(value)
}

// WriteEmptyTagBuffer writes an empty tag buffer
// It is not shown in field encoder's output
// It is done so because decoder also does not print tag buffer (unless an error is encountered)
func (e *FieldEncoder) WriteEmptyTagBuffer() {
	e.encoder.WriteEmptyTagBuffer()
}
