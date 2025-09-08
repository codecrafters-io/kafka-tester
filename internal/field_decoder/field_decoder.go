package field_decoder

import (
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Field struct {
	Path  field_path.FieldPath
	Value value.KafkaProtocolValue
}

type FieldDecoder struct {
	currentPathContexts []string
	decoder             *decoder.Decoder
	decodedFields       []Field
}

type FieldDecoderError struct {
	Message string
	Path    field_path.FieldPath
}

func (e *FieldDecoderError) Error() string {
	return e.Message
}

func NewFieldDecoder(bytes []byte) *FieldDecoder {
	return &FieldDecoder{
		currentPathContexts: []string{},
		decodedFields:       []Field{},
		decoder:             decoder.NewDecoder(bytes),
	}
}

func (d *FieldDecoder) DecodedFields() []Field {
	return d.decodedFields
}

func (d *FieldDecoder) PushPathContext(pathContext string) {
	d.currentPathContexts = append(d.currentPathContexts, pathContext)
}

func (d *FieldDecoder) PopPathContext() {
	d.currentPathContexts = d.currentPathContexts[:len(d.currentPathContexts)-1]
}

func (d *FieldDecoder) ReadCompactArrayLength(path string) (value.CompactArrayLength, *FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadCompactArrayLength()
	if err != nil {
		return value.CompactArrayLength{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt16(path string) (value.Int16, *FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt16()
	if err != nil {
		return value.Int16{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt32(path string) (value.Int32, *FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt32()
	if err != nil {
		return value.Int32{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ConsumeTagBuffer() *FieldDecoderError {
	d.PushPathContext("TAG_BUFFER")
	defer d.PopPathContext()

	if err := d.decoder.ConsumeTagBuffer(); err != nil {
		return d.wrapError(err)
	}

	return nil
}

func (d *FieldDecoder) RemainingBytesCount() uint64 {
	return d.decoder.RemainingBytesCount()
}

func (d *FieldDecoder) currentPath() field_path.FieldPath {
	return field_path.NewFieldPath(strings.Join(d.currentPathContexts, "."))
}

func (d *FieldDecoder) appendDecodedField(decodedValue value.KafkaProtocolValue) {
	d.decodedFields = append(d.decodedFields, Field{
		Value: decodedValue,
		Path:  d.currentPath(),
	})
}

func (d *FieldDecoder) wrapError(err error) *FieldDecoderError {
	// If we've already wrapped the error, preserve the nested path
	if fieldDecoderError, ok := err.(*FieldDecoderError); ok {
		return fieldDecoderError
	}

	return &FieldDecoderError{
		Message: err.Error(),
		Path:    d.currentPath(),
	}
}
