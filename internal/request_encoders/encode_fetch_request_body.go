package request_encoders

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

func encodeFetchRequestBody(requestBody kafkaapi.FetchRequestBody, encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Body")
	defer encoder.PopPathContext()

	encoder.WriteInt32Field("MaxWaitMS", requestBody.MaxWaitMS)
	encoder.WriteInt32Field("MinBytes", requestBody.MinBytes)
	encoder.WriteInt32Field("MaxBytes", requestBody.MaxBytes)
	encoder.WriteInt8Field("IsolationLevel", requestBody.IsolationLevel)
	encoder.WriteInt32Field("SessionID", requestBody.SessionId)
	encoder.WriteInt32Field("SessionEpoch", requestBody.SessionEpoch)

	// Encode topics and forgotten topics
	encodeCompactArray(requestBody.Topics, encoder, "Topics", encodeFetchRequestTopic)
	encodeCompactArray(requestBody.ForgottenTopics, encoder, "ForgottenTopics", encodeForgottenTopic)

	encoder.WriteCompactStringField("RackID", requestBody.RackId)
	encoder.WriteEmptyTagBuffer()
}

func encodeFetchRequestTopic(topic kafkaapi.Topic, encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Topic")
	defer encoder.PopPathContext()

	encoder.WriteUUIDField("UUID", topic.UUID)

	encodeCompactArray(topic.Partitions, encoder, "Partitions", encodeTopicPartition)

	encoder.WriteEmptyTagBuffer()
}

func encodeTopicPartition(partition kafkaapi.Partition, encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Partition")
	defer encoder.PopPathContext()

	encoder.WriteInt32Field("ID", partition.ID)
	encoder.WriteInt32Field("CurrentLeaderEpoch", partition.CurrentLeaderEpoch)
	encoder.WriteInt64Field("FetchOffset", partition.FetchOffset)
	encoder.WriteInt32Field("LastFetchedOffset", partition.LastFetchedOffset)
	encoder.WriteInt64Field("LogStartOffset", partition.LogStartOffset)
	encoder.WriteInt32Field("PartitionMaxBytes", partition.PartitionMaxBytes)
	encoder.WriteEmptyTagBuffer()
}

func encodeForgottenTopic(forgottenTopic kafkaapi.ForgottenTopic, encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("ForgottenTopic")
	defer encoder.PopPathContext()

	// Encode UUID
	encoder.WriteUUIDField("UUID", forgottenTopic.UUID)

	// Encode partitionIds
	partitionIdsKafkaValue := make([]value.KafkaProtocolValue, len(forgottenTopic.PartitionIds))

	for i, partitionId := range forgottenTopic.PartitionIds {
		partitionIdsKafkaValue[i] = partitionId
	}

	encoder.WriteCompactArrayOfValuesField("Partitions", partitionIdsKafkaValue)
}
