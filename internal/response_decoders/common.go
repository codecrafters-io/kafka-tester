package response_decoders

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

func decodeV0Header(decoder *field_decoder.FieldDecoder) (headers.ResponseHeader, field_decoder.FieldDecoderError) {
	correlationId, err := decoder.ReadInt32Field("Header.CorrelationID")
	if err != nil {
		return headers.ResponseHeader{}, err
	}

	return headers.ResponseHeader{
		Version:       0,
		CorrelationId: value.MustBeInt32(correlationId.Value),
	}, nil
}

func decodeV1Header(decoder *field_decoder.FieldDecoder) (headers.ResponseHeader, field_decoder.FieldDecoderError) {
	correlationId, err := decoder.ReadInt32Field("Header.CorrelationID")
	if err != nil {
		return headers.ResponseHeader{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return headers.ResponseHeader{}, err
	}

	return headers.ResponseHeader{
		Version:       1,
		CorrelationId: value.MustBeInt32(correlationId.Value),
	}, nil
}

func decodeCompactArray[T any](decoder *field_decoder.FieldDecoder, decodeFunc func(*field_decoder.FieldDecoder) (T, field_decoder.FieldDecoderError), path string) ([]T, field_decoder.FieldDecoderError) {
	decoder.PushPathContext(path)
	defer decoder.PopPathContext()

	lengthValue, err := decoder.ReadCompactArrayLengthField("Length")
	if err != nil {
		return nil, err
	}

	lengthValueAsCompactArrayLength := value.MustBeCompactArrayLength(lengthValue.Value)
	elements := make([]T, lengthValueAsCompactArrayLength.ActualLength())

	for i := 0; i < int(lengthValueAsCompactArrayLength.ActualLength()); i++ {
		decoder.PushPathContext(fmt.Sprintf("%s[%d]", path, i))
		element, err := decodeFunc(decoder)
		decoder.PopPathContext()

		if err != nil {
			return nil, err
		}

		elements[i] = element
	}

	return elements, nil
}

func decodeArray[T any](decoder *field_decoder.FieldDecoder, decodeFunc func(*field_decoder.FieldDecoder) (T, field_decoder.FieldDecoderError), path string) ([]T, field_decoder.FieldDecoderError) {
	decoder.PushPathContext(path)
	defer decoder.PopPathContext()

	lengthValue, err := decoder.ReadInt32Field("Length")
	if err != nil {
		return nil, err
	}

	lengthValueAsInt32 := value.MustBeInt32(lengthValue.Value)
	if lengthValueAsInt32.Value < 0 {
		// Null array
		if lengthValueAsInt32.Value == -1 {
			return nil, nil
		}
		return nil, decoder.GetDecoderErrorForField(
			fmt.Errorf("Expected array length to be -1 or a non-negative number, got %d", lengthValueAsInt32.Value),
			lengthValue,
		)
	}

	elements := make([]T, lengthValueAsInt32.Value)

	for i := 0; i < int(lengthValueAsInt32.Value); i++ {
		decoder.PushPathContext(fmt.Sprintf("%s[%d]", path, i))
		element, err := decodeFunc(decoder)
		decoder.PopPathContext()

		if err != nil {
			return nil, err
		}

		elements[i] = element
	}

	return elements, nil
}

// decodeCompactRecordBatches decodes RecordBatch data structure in Kafka
// This function and its helper functions will be used across future extensions as well

func decodeCompactRecordBatches(decoder *field_decoder.FieldDecoder, path string) (kafkaapi.RecordBatches, field_decoder.FieldDecoderError) {
	decoder.PushPathContext(path)
	defer decoder.PopPathContext()

	recordBatchesCompactSize, err := decoder.ReadCompactRecordSizeField("Size")

	if err != nil {
		return nil, err
	}

	recordBatchesSize := value.MustBeCompactRecordSize(recordBatchesCompactSize.Value)

	if decoder.RemainingBytesCount() < recordBatchesSize.ActualSize() {
		errorMessage := fmt.Errorf(
			"RecordBatch byte count was decoded as %d bytes, got %d bytes remaining",
			recordBatchesSize.ActualSize(),
			decoder.RemainingBytesCount(),
		)
		return kafkaapi.RecordBatches{}, decoder.GetDecoderErrorForField(errorMessage, recordBatchesCompactSize)
	}

	recordBatchesStartOffset := decoder.ReadBytesCount()

	allRecordBatches := kafkaapi.RecordBatches{}

	index := 0
	for decoder.ReadBytesCount() < (recordBatchesStartOffset + recordBatchesSize.ActualSize()) {
		recordBatch, err := DecodeCompactRecordBatch(decoder, fmt.Sprintf("RecordBatches[%d]", index))
		if err != nil {
			return nil, err
		}
		allRecordBatches = append(allRecordBatches, recordBatch)
		index += 1
	}

	// verify record batch size
	if decoder.ReadBytesCount() != (recordBatchesStartOffset + recordBatchesSize.ActualSize()) {
		errorMessage := fmt.Errorf(
			"Expected RecordBatch byte count to be %d, got %d instead",
			(decoder.ReadBytesCount() - recordBatchesStartOffset),
			recordBatchesSize.ActualSize(),
		)
		return nil, decoder.GetDecoderErrorForField(errorMessage, recordBatchesCompactSize)
	}

	return allRecordBatches, nil
}

func DecodeCompactRecordBatch(decoder *field_decoder.FieldDecoder, path string) (kafkaapi.RecordBatch, field_decoder.FieldDecoderError) {
	decoder.PushPathContext(path)
	defer decoder.PopPathContext()

	baseOffset, err := decoder.ReadInt64Field("Offset")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	batchLength, err := decoder.ReadInt32Field("Length")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	recordBatchStartOffset := decoder.ReadBytesCount()

	partitionLeaderEpoch, err := decoder.ReadInt32Field("PartitionLeaderEpoch")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	magicByte, err := decoder.ReadInt8Field("MagicByte")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	crc, err := decoder.ReadInt32Field("CRC")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	attributes, err := decoder.ReadInt16Field("Attributes")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	lastOffsetDelta, err := decoder.ReadInt32Field("LastOffsetDelta")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	firstTimeStamp, err := decoder.ReadInt64Field("FirstTimeStamp")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	maxTimeStamp, err := decoder.ReadInt64Field("MaxTimeStamp")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	producerId, err := decoder.ReadInt64Field("ProducerID")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	producerEpoch, err := decoder.ReadInt16Field("ProducerEpoch")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	baseSequence, err := decoder.ReadInt32Field("BaseSequence")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	records, err := decodeArray(decoder, decodeRecord, "Records")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	recordBatchEndOffset := decoder.ReadBytesCount()

	decodedRecordBatch := kafkaapi.RecordBatch{
		BaseOffset:           value.MustBeInt64(baseOffset.Value),
		BatchLength:          value.MustBeInt32(batchLength.Value),
		PartitionLeaderEpoch: value.MustBeInt32(partitionLeaderEpoch.Value),
		Magic:                value.MustBeInt8(magicByte.Value),
		CRC:                  value.MustBeInt32(crc.Value),
		Attributes:           value.MustBeInt16(attributes.Value),
		LastOffsetDelta:      value.MustBeInt32(lastOffsetDelta.Value),
		FirstTimestamp:       value.MustBeInt64(firstTimeStamp.Value),
		MaxTimestamp:         value.MustBeInt64(maxTimeStamp.Value),
		ProducerId:           value.MustBeInt64(producerId.Value),
		ProducerEpoch:        value.MustBeInt16(producerEpoch.Value),
		BaseSequence:         value.MustBeInt32(baseSequence.Value),
		Records:              records,
	}

	// verify crc
	crcOK := decodedRecordBatch.IsCRCValueOk()
	if !crcOK {
		errorMessage := fmt.Errorf(
			"Expected CRC value for the record batch to be %s, got %s instead",
			decodedRecordBatch.GetComputedCRCValue(),
			decodedRecordBatch.CRC,
		)
		return kafkaapi.RecordBatch{}, decoder.GetDecoderErrorForField(errorMessage, crc)
	}

	// verify length
	batchLengthAsInt32 := value.MustBeInt32(batchLength.Value)

	if recordBatchEndOffset-recordBatchStartOffset != uint64(batchLengthAsInt32.Value) {
		errorMessage := fmt.Errorf(
			"Expected RecordBatch length to be %d (actual record length), got %d",
			(recordBatchEndOffset - recordBatchStartOffset),
			batchLengthAsInt32.Value,
		)
		return kafkaapi.RecordBatch{}, decoder.GetDecoderErrorForField(errorMessage, batchLength)
	}

	return decodedRecordBatch, nil
}

func decodeRecord(decoder *field_decoder.FieldDecoder) (kafkaapi.Record, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("Record")
	defer decoder.PopPathContext()

	recordLength, err := decoder.ReadVarint("Length")
	if err != nil {
		return kafkaapi.Record{}, err
	}
	recordLengthAsVarint := value.MustBeVarint(recordLength.Value)

	recordStartOffset := decoder.ReadBytesCount()

	attributes, err := decoder.ReadInt8Field("Attributes")
	if err != nil {
		return kafkaapi.Record{}, err
	}

	timestampDelta, err := decoder.ReadVarint("TimestampDelta")
	if err != nil {
		return kafkaapi.Record{}, err
	}

	offsetDelta, err := decoder.ReadVarint("OffsetDelta")
	if err != nil {
		return kafkaapi.Record{}, err
	}

	keyLength, err := decoder.ReadVarint("KeyLength")
	if err != nil {
		return kafkaapi.Record{}, err
	}

	keyLengthAsVarInt := value.MustBeVarint(keyLength.Value)
	recordKey := value.RawBytes{}

	if keyLengthAsVarInt.Value > -1 {
		keyField, err := decoder.ReadRawBytes("Key", int(keyLengthAsVarInt.Value))

		if err != nil {
			return kafkaapi.Record{}, err
		}

		recordKey = value.MustBeRawBytes(keyField.Value)
	}

	valueLength, err := decoder.ReadVarint("ValueLength")

	if err != nil {
		return kafkaapi.Record{}, err
	}

	valueLengthAsVarInt := value.MustBeVarint(valueLength.Value)
	recordValue := value.RawBytes{}

	if valueLengthAsVarInt.Value > -1 {
		recordValueField, err := decoder.ReadRawBytes("Value", int(valueLengthAsVarInt.Value))

		if err != nil {
			return kafkaapi.Record{}, err
		}

		recordValue = value.MustBeRawBytes(recordValueField.Value)
	}

	headersLength, err := decoder.ReadVarint("HeadersLength")
	if err != nil {
		return kafkaapi.Record{}, err
	}

	headersLengthAsVarInt := value.MustBeVarint(headersLength.Value)

	var headers []kafkaapi.RecordHeader

	if headersLengthAsVarInt.Value != -1 {
		headers = make([]kafkaapi.RecordHeader, headersLengthAsVarInt.Value)
		for i := 0; i < int(headersLengthAsVarInt.Value); i++ {
			header, err := decodeRecordHeader(decoder)
			if err != nil {
				return kafkaapi.Record{}, err
			}
			headers[i] = header
		}
	}

	recordEndOffset := decoder.ReadBytesCount()

	if recordEndOffset-recordStartOffset != uint64(recordLengthAsVarint.Value) {
		errorMessage := fmt.Errorf(
			"Expected record length to be %d(actual size of record), got %d instead.",
			(recordEndOffset - recordStartOffset),
			recordLengthAsVarint.Value,
		)
		return kafkaapi.Record{}, decoder.GetDecoderErrorForField(errorMessage, recordLength)
	}

	return kafkaapi.Record{
		Size:           recordLengthAsVarint,
		Attributes:     value.MustBeInt8(attributes.Value),
		TimestampDelta: value.MustBeVarint(timestampDelta.Value),
		OffsetDelta:    value.MustBeVarint(offsetDelta.Value),
		Key:            recordKey,
		Value:          recordValue,
		Headers:        headers,
	}, nil
}

func decodeRecordHeader(decoder *field_decoder.FieldDecoder) (kafkaapi.RecordHeader, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("RecordHeader")
	defer decoder.PopPathContext()

	keyLength, err := decoder.ReadVarint("KeyLength")
	if err != nil {
		return kafkaapi.RecordHeader{}, err
	}

	keyLengthAsVarint := value.MustBeVarint(keyLength.Value)

	keyBytes, err := decoder.ReadRawBytes("Key", int(keyLengthAsVarint.Value))
	if err != nil {
		return kafkaapi.RecordHeader{}, err
	}

	valueLength, err := decoder.ReadVarint("ValueLength")
	if err != nil {
		return kafkaapi.RecordHeader{}, err
	}

	valueLengthAsVarint := value.MustBeVarint(valueLength.Value)

	valueBytes, err := decoder.ReadRawBytes("Value", int(valueLengthAsVarint.Value))
	if err != nil {
		return kafkaapi.RecordHeader{}, err
	}

	return kafkaapi.RecordHeader{
		Key:   value.MustBeRawBytes(keyBytes.Value),
		Value: value.MustBeRawBytes(valueBytes.Value),
	}, nil
}
