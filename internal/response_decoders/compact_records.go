package response_decoders

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

// decodeCompactRecordBatches decodes RecordBatch data structure in Kafka
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

	batchLengthAsInt32 := value.MustBeInt32(batchLength.Value)

	if batchLengthAsInt32.Value <= 0 {
		return kafkaapi.RecordBatch{}, decoder.GetDecoderErrorForField(
			fmt.Errorf("Expected RecordBatch length to be positive, got %d", batchLengthAsInt32.Value),
			batchLength,
		)
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

	// Check record length
	if recordLengthAsVarint.Value <= 0 {
		return kafkaapi.Record{}, decoder.GetDecoderErrorForField(
			fmt.Errorf("Expected record length to be positive, got %d", recordLengthAsVarint.Value),
			recordLength,
		)
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

	keyLengthAsVarint := value.MustBeVarint(keyLength.Value)

	// -1 is reserved for null key
	if keyLengthAsVarint.Value < -1 {
		return kafkaapi.Record{}, decoder.GetDecoderErrorForField(
			fmt.Errorf("Expected the length of record key to be greater than or equal to -1, got %d", keyLengthAsVarint.Value),
			keyLength,
		)
	}

	// Null key
	recordKey := value.RawBytes{}

	if keyLengthAsVarint.Value == 0 {
		// Empty key
		recordKey = value.NewEmptyRawBytes()
	} else if keyLengthAsVarint.Value > 0 {
		// Non empty key
		keyField, err := decoder.ReadRawBytes("Key", int(keyLengthAsVarint.Value))

		if err != nil {
			return kafkaapi.Record{}, err
		}

		recordKey = value.MustBeRawBytes(keyField.Value)
	}

	valueLength, err := decoder.ReadVarint("ValueLength")

	if err != nil {
		return kafkaapi.Record{}, err
	}

	valueLengthAsVarint := value.MustBeVarint(valueLength.Value)

	if valueLengthAsVarint.Value < -1 {
		return kafkaapi.Record{}, decoder.GetDecoderErrorForField(
			fmt.Errorf("Expected the length of record value to be greater than or equal to -1, got %d", valueLengthAsVarint.Value),
			valueLength,
		)
	}

	// Initialize record value as null
	recordValue := value.RawBytes{}

	// Value is empty for 0 length and non-empty for positive length
	if valueLengthAsVarint.Value == 0 {
		recordValue = value.NewEmptyRawBytes()
	} else if valueLengthAsVarint.Value > 0 {
		recordValueField, err := decoder.ReadRawBytes("Value", int(valueLengthAsVarint.Value))

		if err != nil {
			return kafkaapi.Record{}, err
		}

		recordValue = value.MustBeRawBytes(recordValueField.Value)
	}

	headersArrayLength, err := decoder.ReadVarint("HeadersLength")
	if err != nil {
		return kafkaapi.Record{}, err
	}

	headersArrayLengthAsVarint := value.MustBeVarint(headersArrayLength.Value)

	if headersArrayLengthAsVarint.Value < -1 {
		return kafkaapi.Record{}, decoder.GetDecoderErrorForField(
			fmt.Errorf("Expected length of record headers array to be greater than or equal to -1, got %d", headersArrayLengthAsVarint.Value),
			headersArrayLength,
		)
	}

	// Null headers array
	var headers []kafkaapi.RecordHeader

	if headersArrayLengthAsVarint.Value == 0 {
		// Empty array in case of 0 ength
		headers = make([]kafkaapi.RecordHeader, 0)
	} else if headersArrayLengthAsVarint.Value > 0 {
		headers = make([]kafkaapi.RecordHeader, headersArrayLengthAsVarint.Value)
		for i := 0; i < int(headersArrayLengthAsVarint.Value); i++ {
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

	// Check key length value
	if keyLengthAsVarint.Value < -1 {
		return kafkaapi.RecordHeader{}, decoder.GetDecoderErrorForField(
			fmt.Errorf("Expected key length of record header to be greater than or equal to -1, got %d", keyLengthAsVarint.Value),
			keyLength,
		)
	}

	keyBytes := value.RawBytes{}

	if keyLengthAsVarint.Value == 0 {
		keyBytes = value.NewEmptyRawBytes()
	} else if keyLengthAsVarint.Value > 0 {
		keyBytesField, err := decoder.ReadRawBytes("Key", int(keyLengthAsVarint.Value))

		if err != nil {
			return kafkaapi.RecordHeader{}, err
		}

		keyBytes = value.MustBeRawBytes(keyBytesField.Value)
	}

	valueLength, err := decoder.ReadVarint("ValueLength")
	if err != nil {
		return kafkaapi.RecordHeader{}, err
	}

	valueLengthAsVarint := value.MustBeVarint(valueLength.Value)

	if valueLengthAsVarint.Value < -1 {
		return kafkaapi.RecordHeader{}, decoder.GetDecoderErrorForField(
			fmt.Errorf("Expected the value length of record header to be greater than or equal to -1, got %d", valueLengthAsVarint.Value),
			valueLength,
		)
	}

	valueBytes := value.RawBytes{}

	if valueLengthAsVarint.Value == 0 {
		valueBytes = value.NewEmptyRawBytes()
	} else if valueLengthAsVarint.Value > 0 {
		valueBytesField, err := decoder.ReadRawBytes("Value", int(valueLengthAsVarint.Value))
		if err != nil {
			return kafkaapi.RecordHeader{}, err
		}

		valueBytes = value.MustBeRawBytes(valueBytesField.Value)
	}

	return kafkaapi.RecordHeader{
		Key:   keyBytes,
		Value: valueBytes,
	}, nil
}
