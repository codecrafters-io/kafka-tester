package builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type DescribeTopicPartitionsRequestBuilder struct {
	topicNames             []string
	responsePartitionLimit int32
	correlationId          int32
}

func NewDescribeTopicPartitionsRequestBuilder() *DescribeTopicPartitionsRequestBuilder {
	return &DescribeTopicPartitionsRequestBuilder{}
}

func (b *DescribeTopicPartitionsRequestBuilder) WithCorrelationId(correlationId int32) *DescribeTopicPartitionsRequestBuilder {
	b.correlationId = correlationId
	return b
}

func (b *DescribeTopicPartitionsRequestBuilder) WithTopicNames(topicNames []string) *DescribeTopicPartitionsRequestBuilder {
	b.topicNames = topicNames
	return b
}

func (b *DescribeTopicPartitionsRequestBuilder) WithResponsePartitionLimit(responsePartitionLimit int32) *DescribeTopicPartitionsRequestBuilder {
	b.responsePartitionLimit = responsePartitionLimit
	return b
}

func (b *DescribeTopicPartitionsRequestBuilder) Build() kafkaapi.DescribeTopicPartitionsRequest {
	topicNames := make([]value.CompactString, len(b.topicNames))
	for i, topicName := range b.topicNames {
		topicNames[i] = value.CompactString{
			Value: topicName,
		}
	}

	return kafkaapi.DescribeTopicPartitionsRequest{
		// Always send v0 of DescribeTopicPartitions Request
		Header: NewRequestHeaderBuilder().BuildDescribeTopicPartitionsHeader(b.correlationId),
		Body: kafkaapi.DescribeTopicPartitionsRequestBody{
			TopicNames:             topicNames,
			ResponsePartitionLimit: value.Int32{Value: b.responsePartitionLimit},
		},
	}
}
