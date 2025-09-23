package response_decoders

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

func DecodeProduceResponse(decoder *field_decoder.FieldDecoder) (
	kafkaapi.ProduceResponse,
	field_decoder.FieldDecoderError,
) {
	decoder.PushPathContext("ProduceResponse")
	defer decoder.PopPathContext()

	header, err := decodeV1Header(decoder)
	if err != nil {
		return kafkaapi.ProduceResponse{}, err
	}

	body, err := decodeProduceResponseBody(decoder)
	if err != nil {
		return kafkaapi.ProduceResponse{}, err
	}
	return kafkaapi.ProduceResponse{
		Header: header,
		Body:   body,
	}, nil

}

func decodeProduceResponseBody(decoder *field_decoder.FieldDecoder) (kafkaapi.ProduceResponseBody, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("Body")
	defer decoder.PopPathContext()

	topics, err := decodeCompactArray(decoder, decodeProduceResponseTopicData, "Topics")
	if err != nil {
		return kafkaapi.ProduceResponseBody{}, err
	}

	throttleTimeMs, err := decoder.ReadInt32Field("ThrottleTimeMS")
	if err != nil {
		return kafkaapi.ProduceResponseBody{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.ProduceResponseBody{}, err
	}

	return kafkaapi.ProduceResponseBody{
		Topics:         topics,
		ThrottleTimeMs: throttleTimeMs,
	}, nil
}

func decodeProduceResponseTopicData(decoder *field_decoder.FieldDecoder) (kafkaapi.ProduceResponseTopicData, field_decoder.FieldDecoderError) {
	name, err := decoder.ReadCompactStringField("Name")
	if err != nil {
		return kafkaapi.ProduceResponseTopicData{}, err
	}

	partitions, err := decodeCompactArray(decoder, decodeProduceResponsePartitionData, "Partitions")
	if err != nil {
		return kafkaapi.ProduceResponseTopicData{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.ProduceResponseTopicData{}, err
	}

	return kafkaapi.ProduceResponseTopicData{
		Name:       name,
		Partitions: partitions,
	}, nil
}

func decodeProduceResponsePartitionData(decoder *field_decoder.FieldDecoder) (kafkaapi.ProduceResponsePartitionData, field_decoder.FieldDecoderError) {
	id, err := decoder.ReadInt32Field("ID")
	if err != nil {
		return kafkaapi.ProduceResponsePartitionData{}, err
	}

	errorCode, err := decoder.ReadInt16Field("ErrorCode")
	if err != nil {
		return kafkaapi.ProduceResponsePartitionData{}, err
	}

	baseOffset, err := decoder.ReadInt64Field("BaseOffset")
	if err != nil {
		return kafkaapi.ProduceResponsePartitionData{}, err
	}

	logAppendTimeMs, err := decoder.ReadInt64Field("LogAppendTimeMs")
	if err != nil {
		return kafkaapi.ProduceResponsePartitionData{}, err
	}

	logStartOffset, err := decoder.ReadInt64Field("LogStartOffset")
	if err != nil {
		return kafkaapi.ProduceResponsePartitionData{}, err
	}

	recordErrors, err := decodeCompactArray(decoder, decodeProduceResponseRecordErrorsData, "RecordErrors")
	if err != nil {
		return kafkaapi.ProduceResponsePartitionData{}, err
	}

	errorMessage, err := decoder.ReadCompactNullableStringField("ErrorMessage")
	if err != nil {
		return kafkaapi.ProduceResponsePartitionData{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.ProduceResponsePartitionData{}, err
	}

	return kafkaapi.ProduceResponsePartitionData{
		Id:              id,
		ErrorCode:       errorCode,
		BaseOffset:      baseOffset,
		LogAppendTimeMs: logAppendTimeMs,
		LogStartOffset:  logStartOffset,
		RecordErrors:    recordErrors,
		ErrorMessage:    errorMessage,
	}, nil
}

func decodeProduceResponseRecordErrorsData(decoder *field_decoder.FieldDecoder) (kafkaapi.ProduceResponseRecordErrorData, field_decoder.FieldDecoderError) {
	batchIndex, err := decoder.ReadInt32Field("BatchIndex")
	if err != nil {
		return kafkaapi.ProduceResponseRecordErrorData{}, err
	}

	errorMessage, err := decoder.ReadCompactNullableStringField("ErrorMessage")
	if err != nil {
		return kafkaapi.ProduceResponseRecordErrorData{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.ProduceResponseRecordErrorData{}, err
	}

	return kafkaapi.ProduceResponseRecordErrorData{
		BatchIndex:   batchIndex,
		ErrorMessage: errorMessage,
	}, nil
}
