package field_decoder

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field"
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type pathContext struct {
	pathName string
	offset   uint64 // the offset at which the pathName was added to the context
}

type FieldDecoder struct {
	currentPathContexts []pathContext
	decoder             *decoder.Decoder
	decodedFields       []field.Field
}

func NewFieldDecoder(bytes []byte) *FieldDecoder {
	return &FieldDecoder{
		currentPathContexts: []pathContext{},
		decodedFields:       []field.Field{},
		decoder:             decoder.NewDecoder(bytes),
	}
}

func (d *FieldDecoder) ReadBytesCount() uint64 {
	return d.decoder.ReadBytesCount()
}

func (d *FieldDecoder) DecodedFields() []field.Field {
	return d.decodedFields
}

func (d *FieldDecoder) PushPathContext(pathName string) {
	d.currentPathContexts = append(d.currentPathContexts, pathContext{
		pathName: pathName,
		offset:   d.ReadBytesCount(), // capture current offset for the context
	})
}

func (d *FieldDecoder) PopPathContext() {
	d.currentPathContexts = d.currentPathContexts[:len(d.currentPathContexts)-1]
}

func (d *FieldDecoder) getStartOffsetOfLastDecodedField() int {
	return d.decodedFields[len(d.decodedFields)-1].StartOffset
}

func (d *FieldDecoder) getEndOffsetOfLastDecodedField() int {
	return d.decodedFields[len(d.decodedFields)-1].EndOffset
}

func (d *FieldDecoder) ReadCompactArrayLengthField(path string) (value.AugmentedCompactArrayLength, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadCompactArrayLength()
	if err != nil {
		return value.AugmentedCompactArrayLength{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedCompactArrayLength{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadCompactRecordSizeField(path string) (value.AugmentedCompactRecordSize, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadCompactRecordSize()
	if err != nil {
		return value.AugmentedCompactRecordSize{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedCompactRecordSize{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadUnsignedVarInt(path string) (value.AugmentedUnsignedVarint, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadUnsignedVarint()

	if err != nil {
		return value.AugmentedUnsignedVarint{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedUnsignedVarint{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadVarint(path string) (value.AugmentedVarint, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadVarint()

	if err != nil {
		return value.AugmentedVarint{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedVarint{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadCompactNullableStringField(path string) (value.AugmentedCompactNullableString, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	lengthValue, err := d.decoder.ReadCompactArrayLength()
	if err != nil {
		return value.AugmentedCompactNullableString{}, d.wrapError(err)
	}

	rawBytes, err := d.decoder.ReadRawBytes(int(lengthValue.ActualLength()))

	if err != nil {
		return value.AugmentedCompactNullableString{}, d.wrapError(err)
	}

	stringValue := string(rawBytes.Value)

	decodedValue := value.CompactNullableString{
		Value: &stringValue,
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedCompactNullableString{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadCompactStringField(path string) (value.AugmentedCompactString, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	lengthValue, err := d.decoder.ReadCompactArrayLength()

	if err != nil {
		return value.AugmentedCompactString{}, d.wrapError(err)
	}

	if lengthValue.Value == 0 {
		return value.AugmentedCompactString{}, d.wrapError(fmt.Errorf("Compact string length cannot be 0"))
	}

	rawBytes, err := d.decoder.ReadRawBytes(int(lengthValue.ActualLength()))

	if err != nil {
		return value.AugmentedCompactString{}, d.wrapError(err)
	}

	stringValue := string(rawBytes.Value)

	decodedValue := value.CompactString{
		Value: stringValue,
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedCompactString{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadUUIDField(path string) (value.AugmentedUUID, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadUUID()
	if err != nil {
		return value.AugmentedUUID{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedUUID{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadBooleanField(path string) (value.AugmentedBoolean, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadBoolean()
	if err != nil {
		return value.AugmentedBoolean{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)
	return value.AugmentedBoolean{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadInt16Field(path string) (value.AugmentedInt16, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt16()
	if err != nil {
		return value.AugmentedInt16{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedInt16{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadInt8Field(path string) (value.AugmentedInt8, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt8()
	if err != nil {
		return value.AugmentedInt8{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedInt8{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadInt32Field(path string) (value.AugmentedInt32, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt32()
	if err != nil {
		return value.AugmentedInt32{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedInt32{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadInt64Field(path string) (value.AugmentedInt64, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt64()

	if err != nil {
		return value.AugmentedInt64{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedInt64{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ReadRawBytes(path string, count int) (value.AugmentedRawBytes, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadRawBytes(count)
	if err != nil {
		return value.AugmentedRawBytes{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return value.AugmentedRawBytes{
		Path:        d.currentPath().String(),
		KafkaValue:  decodedValue,
		StartOffset: d.getStartOffsetOfLastDecodedField(),
		EndOffset:   d.getEndOffsetOfLastDecodedField(),
	}, nil
}

func (d *FieldDecoder) ConsumeTagBufferField() FieldDecoderError {
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
	pathNamesFromContext := []string{}

	for _, pathContext := range d.currentPathContexts {
		pathNamesFromContext = append(pathNamesFromContext, pathContext.pathName)
	}

	return field_path.NewFieldPath(strings.Join(pathNamesFromContext, "."))
}

func (d *FieldDecoder) appendDecodedField(decodedValue value.KafkaProtocolValue) {
	// retrieve most recently added path context's offset so the offset of the decoded field can be the same
	lastPathContextOffset := d.currentPathContexts[len(d.currentPathContexts)-1].offset

	d.decodedFields = append(d.decodedFields, field.Field{
		Value:       decodedValue,
		Path:        d.currentPath(),
		StartOffset: int(lastPathContextOffset),
		// The pointer will have pointed to the next byte already, so we re-adjust the pointer to the previous byte
		EndOffset: int(d.ReadBytesCount() - 1),
	})
}

func (d *FieldDecoder) wrapError(err error) FieldDecoderError {
	// If we've already wrapped the error, preserve the nested path
	if fieldDecoderError, ok := err.(*fieldDecoderErrorImpl); ok {
		return fieldDecoderError
	}

	if decoderError, ok := err.(decoder.DecoderError); ok {
		return &fieldDecoderErrorImpl{
			message:     err.Error(),
			startOffset: decoderError.StartOffset(),
			endOffset:   decoderError.EndOffset(),
			path:        d.currentPath(),
		}
	}

	panic("Codecrafters Internal Error - error is not of type fieldDecoderErrorImpl or DecoderError")
}

func (d *FieldDecoder) WrapErrorForAugmentedValue(err error, augmentedValue value.AugmentedValue) FieldDecoderError {
	return &fieldDecoderErrorImpl{
		message: err.Error(),
		// Use the startOffset, endOffset and path from the augmented value's start and end offset
		startOffset: augmentedValue.GetStartOffset(),
		endOffset:   augmentedValue.GetEndOffset(),
		path:        field_path.NewFieldPath(augmentedValue.GetPath()),
	}
}
