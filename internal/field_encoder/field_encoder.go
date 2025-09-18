package field_encoder

import (
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type EncodedField struct {
	path  field_path.FieldPath
	value value.KafkaProtocolValue
}

// GetPath implements Field interface
func (e *EncodedField) GetPath() field_path.FieldPath {
	return e.path
}

// GetValue implements Field interface
func (e *EncodedField) GetValue() value.KafkaProtocolValue {
	return e.value
}

type FieldEncoder struct {
	currentPathContexts []string
	encoder             *encoder.Encoder
	encodedFields       []EncodedField
}

func NewFieldEncoder() *FieldEncoder {
	return &FieldEncoder{
		currentPathContexts: []string{},
		encodedFields:       []EncodedField{},
		encoder:             encoder.NewEncoder(),
	}
}

func (e *FieldEncoder) EncodedFields() []EncodedField {
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

func (e *FieldEncoder) appendEncodedField(encodedValue value.KafkaProtocolValue) {
	e.encodedFields = append(e.encodedFields, EncodedField{
		value: encodedValue,
		path:  e.currentPath(),
	})
}

func (e *FieldEncoder) Bytes() []byte {
	return e.encoder.Bytes()
}

func (e *FieldEncoder) WriteInt16Field(variableName string, value value.Int16) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteInt16(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteInt32Field(variableName string, value value.Int32) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteInt32(value.Value)
	e.appendEncodedField(value)
}

func (e *FieldEncoder) WriteStringField(variableName string, value value.String) {
	e.PushPathContext(variableName)
	defer e.PopPathContext()
	e.encoder.WriteString(value.Value)
	e.appendEncodedField(value)
}

// WriteEmptyTagBuffer writes an empty tag buffer
// It is not shown in field encoder's output
// It is done so because decoder also does not print tag buffer (unless an error is encountered)
func (e *FieldEncoder) WriteEmptyTagBuffer() {
	e.encoder.WriteEmptyTagBuffer()
}
