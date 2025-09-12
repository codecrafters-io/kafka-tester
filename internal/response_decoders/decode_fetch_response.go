package response_decoders

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

func DecodeFetchResponse(decoder *field_decoder.FieldDecoder) (
	kafkaapi.FetchResponse,
	field_decoder.FieldDecoderError,
) {
	throttleTimeMs, err := decoder.ReadInt32Field("ThrottleTimeMS")
	if err != nil {
		return kafkaapi.FetchResponse{}, err
	}

	errorCode, err := decoder.ReadInt16Field("ErrorCode")
	if err != nil {
		return kafkaapi.FetchResponse{}, err
	}

	sessionId, err := decoder.ReadInt32Field("SessionID")
	if err != nil {
		return kafkaapi.FetchResponse{}, err
	}

	topicResponses, err := decodeCompactArray(decoder, decodeFetchTopic, "Topics")
	if err != nil {
		return kafkaapi.FetchResponse{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.FetchResponse{}, err
	}

	return kafkaapi.FetchResponse{
		ThrottleTimeMs: throttleTimeMs,
		ErrorCode:      errorCode,
		SessionId:      sessionId,
		TopicResponses: topicResponses,
	}, nil
}

func decodeFetchTopic(decoder *field_decoder.FieldDecoder) (kafkaapi.TopicResponse, field_decoder.FieldDecoderError) {
	topicUUID, err := decoder.ReadUUIDField("UUID")
	if err != nil {
		return kafkaapi.TopicResponse{}, err
	}

	partitions, err := decodeCompactArray(decoder, decodeFetchPartition, "Partitions")
	if err != nil {
		return kafkaapi.TopicResponse{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.TopicResponse{}, err
	}

	return kafkaapi.TopicResponse{
		UUID:               topicUUID,
		PartitionResponses: partitions,
	}, nil
}

func decodeFetchPartition(decoder *field_decoder.FieldDecoder) (kafkaapi.PartitionResponse, field_decoder.FieldDecoderError) {
	id, err := decoder.ReadInt32Field("Id")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	errorCode, err := decoder.ReadInt16Field("ErrorCode")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	highWaterMark, err := decoder.ReadInt64Field("HighWaterMark")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	lastStableOffset, err := decoder.ReadInt64Field("LastStableOffset")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	logStartOffset, err := decoder.ReadInt64Field("LogStartOffset")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	abortedTransactions, err := decodeCompactArray(decoder, decodeAbortedTransaction, "AbortedTransactions")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	preferredReadReplica, err := decoder.ReadInt64Field("PreferredReadReplica")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	recordBatchesTotalSize, err := decoder.ReadCompactArrayLengthField("RecordBatchesSize")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	if decoder.RemainingBytesCount() < recordBatchesTotalSize.Value {
		errorMessage := fmt.Errorf("Expected total size of record batches to be %d bytes, got %d bytes", recordBatchesTotalSize.Value, decoder.RemainingBytesCount())
		return kafkaapi.PartitionResponse{}, decoder.WrapError(errorMessage)
	}

	recordBatches, err := decodeRecordBatches(decoder)
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	return kafkaapi.PartitionResponse{
		Id:                  id,
		ErrorCode:           errorCode,
		HighWatermark:       highWaterMark,
		LastStableOffset:    lastStableOffset,
		LogStartOffset:      logStartOffset,
		AbortedTransactions: abortedTransactions,
		PreferedReadReplica: preferredReadReplica,
		RecordBatches:       recordBatches,
	}, nil
}

func decodeAbortedTransaction(decoder *field_decoder.FieldDecoder) (kafkaapi.AbortedTransaction, field_decoder.FieldDecoderError) {
	producerID, err := decoder.ReadInt64Field("ProducerID")
	if err != nil {
		return kafkaapi.AbortedTransaction{}, err
	}

	firstOffset, err := decoder.ReadInt64Field("FirstOffset")
	if err != nil {
		return kafkaapi.AbortedTransaction{}, err
	}

	return kafkaapi.AbortedTransaction{
		ProducerID:  producerID,
		FirstOffset: firstOffset,
	}, nil
}

// For decoding record batches

func decodeRecordBatches(decoder *field_decoder.FieldDecoder) (kafkaapi.RecordBatches, field_decoder.FieldDecoderError) {
	// recordBatches := kafkaapi.RecordBatches{}

	return kafkaapi.RecordBatches{}, nil
}
