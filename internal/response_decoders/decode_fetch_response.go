package response_decoders

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

func DecodeFetchResponse(decoder *field_decoder.FieldDecoder) (
	kafkaapi.FetchResponse,
	field_decoder.FieldDecoderError,
) {
	decoder.PushPathContext("FetchResponse")
	defer decoder.PopPathContext()

	header, err := decodeV1Header(decoder)
	if err != nil {
		return kafkaapi.FetchResponse{}, err
	}

	body, err := decodeFetchResponseBody(decoder)
	if err != nil {
		return kafkaapi.FetchResponse{}, err
	}
	return kafkaapi.FetchResponse{
		Header: header,
		Body:   body,
	}, nil

}

func decodeFetchResponseBody(decoder *field_decoder.FieldDecoder) (kafkaapi.FetchResponseBody, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("Body")
	defer decoder.PopPathContext()

	throttleTimeMs, err := decoder.ReadInt32Field("ThrottleTimeMS")
	if err != nil {
		return kafkaapi.FetchResponseBody{}, err
	}

	errorCode, err := decoder.ReadInt16Field("ErrorCode")
	if err != nil {
		return kafkaapi.FetchResponseBody{}, err
	}

	sessionId, err := decoder.ReadInt32Field("SessionID")
	if err != nil {
		return kafkaapi.FetchResponseBody{}, err
	}

	topicResponses, err := decodeCompactArray(decoder, decodeFetchTopic, "Topics")
	if err != nil {
		return kafkaapi.FetchResponseBody{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.FetchResponseBody{}, err
	}

	return kafkaapi.FetchResponseBody{
		ThrottleTimeMs: throttleTimeMs.KafkaValue,
		ErrorCode:      errorCode.KafkaValue,
		SessionId:      sessionId.KafkaValue,
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
		UUID:               topicUUID.KafkaValue,
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

	preferredReadReplica, err := decoder.ReadInt32Field("PreferredReadReplica")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	recordBatches, err := decodeCompactRecordBatches(decoder, "RecordBatches")
	if err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.PartitionResponse{}, err
	}

	return kafkaapi.PartitionResponse{
		Id:                   id.KafkaValue,
		ErrorCode:            errorCode.KafkaValue,
		HighWatermark:        highWaterMark.KafkaValue,
		LastStableOffset:     lastStableOffset.KafkaValue,
		LogStartOffset:       logStartOffset.KafkaValue,
		AbortedTransactions:  abortedTransactions,
		PreferredReadReplica: preferredReadReplica.KafkaValue,
		RecordBatches:        recordBatches,
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
		ProducerID:  producerID.KafkaValue,
		FirstOffset: firstOffset.KafkaValue,
	}, nil
}
