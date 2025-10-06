package response_decoders

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

func DecodeDescribeTopicPartitionsResponse(decoder *field_decoder.FieldDecoder) (
	kafkaapi.DescribeTopicPartitionsResponse,
	field_decoder.FieldDecoderError,
) {
	decoder.PushPathContext("DescribeTopicPartitionsResponse")
	defer decoder.PopPathContext()

	header, err := decodeV1Header(decoder)
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponse{}, err
	}

	body, err := decodeDescribeTopicPartitionsResponseBody(decoder)
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponse{}, err
	}

	return kafkaapi.DescribeTopicPartitionsResponse{
		Header: header,
		Body:   body,
	}, nil
}

func decodeDescribeTopicPartitionsResponseBody(decoder *field_decoder.FieldDecoder) (kafkaapi.DescribeTopicPartitionsResponseBody, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("Body")
	defer decoder.PopPathContext()

	throttleTimeMs, err := decoder.ReadInt32Field("ThrottleTimeMs")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseBody{}, err
	}

	topics, err := decodeCompactArray(decoder, decodeTopic, "Topics")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseBody{}, err
	}

	cursor, err := decodeCursor(decoder)
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseBody{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseBody{}, err
	}

	return kafkaapi.DescribeTopicPartitionsResponseBody{
		ThrottleTimeMs: throttleTimeMs.KafkaValue,
		Topics:         topics,
		NextCursor:     cursor,
	}, err
}

func decodeTopic(decoder *field_decoder.FieldDecoder) (kafkaapi.DescribeTopicPartitionsResponseTopic, field_decoder.FieldDecoderError) {
	errorCode, err := decoder.ReadInt16Field("ErrorCode")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseTopic{}, err
	}

	// Read compact nullable string
	name, err := decoder.ReadCompactNullableStringField("Name")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseTopic{}, err
	}

	// Read topic UUID
	topicUUID, err := decoder.ReadUUIDField("UUID")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseTopic{}, err
	}

	isInternal, err := decoder.ReadBooleanField("IsInternal")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseTopic{}, err
	}

	partitions, err := decodeCompactArray(decoder, decodePartition, "Partitions")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseTopic{}, err
	}

	topicAuthorizedOperations, err := decoder.ReadInt32Field("TopicAuthorizedOperations")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseTopic{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseTopic{}, err
	}

	return kafkaapi.DescribeTopicPartitionsResponseTopic{
		ErrorCode:                 errorCode.KafkaValue,
		Name:                      name.KafkaValue,
		TopicUUID:                 topicUUID.KafkaValue,
		IsInternal:                isInternal.KafkaValue,
		Partitions:                partitions,
		TopicAuthorizedOperations: topicAuthorizedOperations.KafkaValue,
	}, nil
}

func decodePartition(decoder *field_decoder.FieldDecoder) (kafkaapi.DescribeTopicPartitionsResponsePartition, field_decoder.FieldDecoderError) {
	errorCode, err := decoder.ReadInt16Field("ErrorCode")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	partitionIndex, err := decoder.ReadInt32Field("PartitionIndex")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	leaderId, err := decoder.ReadInt32Field("LeaderId")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	leaderEpoch, err := decoder.ReadInt32Field("LeaderEpoch")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	replicaNodes, err := decodeCompactArray(decoder, decodeReplicaNode, "ReplicaNodes")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	isrNodes, err := decodeCompactArray(decoder, decodeIsrNode, "IsrNodes")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	eligibleLeaderReplicas, err := decodeCompactArray(decoder, decodeEligibleLeaderReplica, "EligibleLeaderReplicas")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	lastKnownELR, err := decodeCompactArray(decoder, decodeLastKnownELRNode, "LastKnownELR")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	offlineReplicas, err := decodeCompactArray(decoder, decodeOfflineReplica, "OfflineReplicas")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.DescribeTopicPartitionsResponsePartition{}, err
	}

	return kafkaapi.DescribeTopicPartitionsResponsePartition{
		ErrorCode:              errorCode.KafkaValue,
		PartitionIndex:         partitionIndex.KafkaValue,
		LeaderId:               leaderId.KafkaValue,
		LeaderEpoch:            leaderEpoch.KafkaValue,
		ReplicaNodes:           replicaNodes,
		IsrNodes:               isrNodes,
		EligibleLeaderReplicas: eligibleLeaderReplicas,
		LastKnownElr:           lastKnownELR,
		OfflineReplicas:        offlineReplicas,
	}, nil
}

func decodeCursor(decoder *field_decoder.FieldDecoder) (kafkaapi.DescribeTopicPartitionsResponseCursor, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("Cursor")
	defer decoder.PopPathContext()

	isCursorPresent, err := decoder.ReadInt8Field("IsCursorPresent")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseCursor{}, err
	}

	if isCursorPresent.KafkaValue.Value == -1 {
		return kafkaapi.DescribeTopicPartitionsResponseCursor{}, nil
	}

	topicName, err := decoder.ReadCompactStringField("TopicName")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseCursor{}, err
	}

	partitionIndex, err := decoder.ReadInt32Field("PartitionIndex")
	if err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseCursor{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.DescribeTopicPartitionsResponseCursor{}, err
	}

	return kafkaapi.DescribeTopicPartitionsResponseCursor{
		TopicName:      topicName.KafkaValue,
		PartitionIndex: partitionIndex.KafkaValue,
	}, nil
}

func decodeReplicaNode(decoder *field_decoder.FieldDecoder) (value.Int32, field_decoder.FieldDecoderError) {
	replicaNode, err := decoder.ReadInt32Field("ReplicaNode")
	return replicaNode.KafkaValue, err
}

func decodeIsrNode(decoder *field_decoder.FieldDecoder) (value.Int32, field_decoder.FieldDecoderError) {
	isrNode, err := decoder.ReadInt32Field("IsrNode")
	return isrNode.KafkaValue, err
}

func decodeEligibleLeaderReplica(decoder *field_decoder.FieldDecoder) (value.Int32, field_decoder.FieldDecoderError) {
	eligibleLeaderReplica, err := decoder.ReadInt32Field("EligibleLeaderReplica")
	return eligibleLeaderReplica.KafkaValue, err
}

func decodeLastKnownELRNode(decoder *field_decoder.FieldDecoder) (value.Int32, field_decoder.FieldDecoderError) {
	lastKnownELRNode, err := decoder.ReadInt32Field("LastKnownELRNode")
	return lastKnownELRNode.KafkaValue, err
}

func decodeOfflineReplica(decoder *field_decoder.FieldDecoder) (value.Int32, field_decoder.FieldDecoderError) {
	offlineReplica, err := decoder.ReadInt32Field("OfflineReplica")
	return offlineReplica.KafkaValue, err
}
