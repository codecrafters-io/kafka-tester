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
		CorrelationId: correlationId,
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
		CorrelationId: correlationId,
	}, nil
}

func decodeCompactArray[T any](decoder *field_decoder.FieldDecoder, decodeFunc func(*field_decoder.FieldDecoder) (T, field_decoder.FieldDecoderError), path string) ([]T, field_decoder.FieldDecoderError) {
	decoder.PushPathContext(path)
	defer decoder.PopPathContext()

	lengthValue, err := decoder.ReadCompactArrayLengthField("Length")
	if err != nil {
		return nil, err
	}

	elements := make([]T, lengthValue.ActualLength())

	for i := 0; i < int(lengthValue.ActualLength()); i++ {
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

	arrayLengthOffset := decoder.ReadBytesCount()
	lengthValue, err := decoder.ReadInt32Field("Length")
	if err != nil {
		return nil, err
	}

	if lengthValue.Value < 0 {
		// Null array
		if lengthValue.Value == -1 {
			return nil, nil
		}
		// Show that length is wrong
		decoder.PushPathContext("Length")
		return nil, decoder.WrapErrorAtOffset(fmt.Errorf("Expected array length to be -1 or a non-negative number, got %d", lengthValue.Value), arrayLengthOffset)
	}

	elements := make([]T, lengthValue.Value)

	for i := 0; i < int(lengthValue.Value); i++ {
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

	if decoder.RemainingBytesCount() < recordBatchesCompactSize.ActualSize() {
		// Report that the size is wrong
		decoder.PushPathContext("Size")
		// However, the offset can be left unchanged because user want to see the bytes after the 'size' bytes
		errorMessage := fmt.Errorf("RecordBatches' total size was decoded as %d bytes, got %d bytes remaining", recordBatchesCompactSize.ActualSize(), decoder.RemainingBytesCount())
		return kafkaapi.RecordBatches{}, decoder.WrapError(errorMessage)
	}

	recordBatchesStartOffset := decoder.ReadBytesCount()

	allRecordBatches := kafkaapi.RecordBatches{}

	index := 0
	for decoder.ReadBytesCount() < (recordBatchesStartOffset + recordBatchesCompactSize.ActualSize()) {
		recordBatch, err := decodeCompactRecordBatch(decoder, fmt.Sprintf("RecordBatches[%d]", index))
		if err != nil {
			return nil, err
		}
		allRecordBatches = append(allRecordBatches, recordBatch)
		index += 1
	}

	// verify record batch size
	if decoder.ReadBytesCount() != (recordBatchesStartOffset + recordBatchesCompactSize.ActualSize()) {
		// Report that size is wrong
		decoder.PushPathContext("Size")

		// But offset should be record batch's start offset
		return nil, decoder.WrapErrorAtOffset(
			fmt.Errorf("Expected Recordbatches' total size to be %d, got %d instead", (decoder.ReadBytesCount()-recordBatchesStartOffset), recordBatchesCompactSize.ActualSize()),
			recordBatchesStartOffset,
		)
	}

	return allRecordBatches, nil
}

func decodeCompactRecordBatch(decoder *field_decoder.FieldDecoder, path string) (kafkaapi.RecordBatch, field_decoder.FieldDecoderError) {
	decoder.PushPathContext(path)
	defer decoder.PopPathContext()

	baseOffset, err := decoder.ReadInt64Field("Offset")
	if err != nil {
		return kafkaapi.RecordBatch{}, err
	}

	lengthOffset := decoder.ReadBytesCount()
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

	crcBytesOffset := decoder.ReadBytesCount()
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
		BaseOffset:           baseOffset,
		BatchLength:          batchLength,
		PartitionLeaderEpoch: partitionLeaderEpoch,
		Magic:                magicByte,
		CRC:                  crc,
		Attributes:           attributes,
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       firstTimeStamp,
		MaxTimestamp:         maxTimeStamp,
		ProducerId:           producerId,
		ProducerEpoch:        producerEpoch,
		BaseSequence:         baseSequence,
		Records:              records,
	}

	// verify crc
	crcOK := decodedRecordBatch.IsCRCValueOk()
	if !crcOK {
		// Report crc error with proper offset
		decoder.PushPathContext("CRC")
		return kafkaapi.RecordBatch{}, decoder.WrapErrorAtOffset(fmt.Errorf("Incorrect CRC value for the record batch"), crcBytesOffset)
	}

	// verify length
	if recordBatchEndOffset-recordBatchStartOffset != uint64(batchLength.Value) {
		// Report length error with bytes pointing to length
		decoder.PushPathContext("Length")
		return kafkaapi.RecordBatch{}, decoder.WrapErrorAtOffset(
			fmt.Errorf("Expected RecordBatch length to be %d (actual record length), got %d", (recordBatchEndOffset-recordBatchStartOffset), batchLength.Value),
			lengthOffset,
		)
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

	key := value.RawBytes{}
	if keyLength.Value > -1 {
		key, err = decoder.ReadRawBytes("Key", int(keyLength.Value))

		if err != nil {
			return kafkaapi.Record{}, err
		}
	}

	valueLength, err := decoder.ReadVarint("ValueLength")

	if err != nil {
		return kafkaapi.Record{}, err
	}

	recordValue := value.RawBytes{}
	if valueLength.Value > -1 {
		recordValue, err = decoder.ReadRawBytes("Value", int(valueLength.Value))
		if err != nil {
			return kafkaapi.Record{}, err
		}
	}

	headersLength, err := decoder.ReadVarint("HeadersLength")
	if err != nil {
		return kafkaapi.Record{}, err
	}

	var headers []kafkaapi.RecordHeader

	if headersLength.Value != -1 {
		headers = make([]kafkaapi.RecordHeader, headersLength.Value)
		for i := 0; i < int(headersLength.Value); i++ {
			header, err := decodeRecordHeader(decoder)
			if err != nil {
				return kafkaapi.Record{}, err
			}
			headers[i] = header
		}
	}

	recordEndOffset := decoder.ReadBytesCount()

	if recordEndOffset-recordStartOffset != uint64(recordLength.Value) {
		// Report length as wrong
		decoder.PushPathContext("Length")
		// Offset should be record's start offset just after 'length' bytes
		return kafkaapi.Record{}, decoder.WrapErrorAtOffset(
			fmt.Errorf("Expected record's length to be %d(actual size of record), got %d instead.", (recordEndOffset-recordStartOffset), recordLength.Value),
			recordStartOffset,
		)
	}

	return kafkaapi.Record{
		Length:         value.Int32{Value: int32(recordLength.Value)},
		Attributes:     attributes,
		TimestampDelta: timestampDelta,
		OffsetDelta:    offsetDelta,
		Key:            key.Value,
		Value:          recordValue.Value,
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

	keyBytes, err := decoder.ReadRawBytes("Key", int(keyLength.Value))
	if err != nil {
		return kafkaapi.RecordHeader{}, err
	}

	valueLength, err := decoder.ReadVarint("ValueLength")
	if err != nil {
		return kafkaapi.RecordHeader{}, err
	}

	valueBytes, err := decoder.ReadRawBytes("Value", int(valueLength.Value))
	if err != nil {
		return kafkaapi.RecordHeader{}, err
	}

	return kafkaapi.RecordHeader{
		Key:   keyBytes,
		Value: valueBytes,
	}, nil
}
