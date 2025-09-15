package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type DescribeTopicPartitionsResponse struct {
	Header headers.ResponseHeader
	Body   DescribeTopicPartitionsResponseBody
}

type DescribeTopicPartitionsResponseBody struct {
	ThrottleTimeMs value.Int32
	Topics         []DescribeTopicPartitionsResponseTopic
	NextCursor     DescribeTopicPartitionsResponseCursor
}

type DescribeTopicPartitionsResponseCursor struct {
	TopicName      value.CompactString
	PartitionIndex value.Int32
}

type DescribeTopicPartitionsResponseTopic struct {
	ErrorCode                 value.Int16
	Name                      value.CompactNullableString
	TopicUUID                 value.UUID
	IsInternal                value.Boolean
	Partitions                []DescribeTopicPartitionsResponsePartition
	TopicAuthorizedOperations value.Int32
}

type DescribeTopicPartitionsResponsePartition struct {
	ErrorCode              value.Int16
	PartitionIndex         value.Int32
	LeaderID               value.Int32
	LeaderEpoch            value.Int32
	ReplicaNodes           []value.Int32
	IsrNodes               []value.Int32
	EligibleLeaderReplicas []value.Int32
	LastKnownELR           []value.Int32
	OfflineReplicas        []value.Int32
}
