package field_encoder

import (
	"fmt"
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

func (e *FieldEncoder) WriteInt8Field(variableName string, value kafka_value.Int8) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteInt8(value.Value)
	e.appendEncodedField(value)
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

func (e *FieldEncoder) WriteInt64Field(variableName string, value kafka_value.Int64) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteInt64(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteStringField(variableName string, value kafka_value.String) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteString(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteRawBytes(variableName string, value kafka_value.RawBytes) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteRawBytes(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteVarint(variableName string, value kafka_value.Varint) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteVarint(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteUvarint(variableName string, value kafka_value.UnsignedVarint) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteUvarint(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteCompactNullableStringField(variableName string, value kafka_value.CompactNullableString) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteCompactNullableString(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteCompactStringField(variableName string, value kafka_value.CompactString) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteCompactString(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteCompactArrayLengthField(variableName string, value kafka_value.CompactArrayLength) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteUvarint(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteCompactArrayOfValuesField(variableName string, values []kafka_value.KafkaProtocolValue) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()

	e.WriteCompactArrayLengthField("Length", kafka_value.NewCompactArrayLength(values))
	for i, value := range values {
		if castedInt32, ok := value.(kafka_value.Int32); ok {
			e.WriteInt32Field(fmt.Sprintf("%s[%d]", variableName, i), castedInt32)
			continue
		}

		panic(fmt.Sprintf("Codecrafters Internal Error - Compact Array of %s cannot be encoded", value.GetType()))
	}
}

func (e *FieldEncoder) WriteUUIDField(variableName string, value kafka_value.UUID) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteUUID(value.Value)
	e.appendEncodedField(value)
}

// WriteEmptyTagBuffer writes an empty tag buffer
// It is not shown in field encoder's output
// It is done so because decoder also does not print tag buffer (unless an error is encountered)
func (e *FieldEncoder) WriteEmptyTagBuffer() {
	e.encoder.WriteEmptyTagBuffer()
}
