package builder

import (
	"math"

	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type FetchRequestPartition struct {
	ID int32
}

type FetchRequestTopic struct {
	UUID       string
	Partitions []FetchRequestPartition
}

type FetchRequestBuilder struct {
	correlationId int32
	sessionId     int32
	topicUUID     string
	partitionID   int32
}

func NewFetchRequestBuilder() *FetchRequestBuilder {
	return &FetchRequestBuilder{}
}

func (b *FetchRequestBuilder) WithCorrelationId(correlationId int32) *FetchRequestBuilder {
	b.correlationId = correlationId
	return b
}

func (b *FetchRequestBuilder) WithSessionId(sessionId int32) *FetchRequestBuilder {
	b.sessionId = sessionId
	return b
}

func (b *FetchRequestBuilder) WithTopicUUID(topicUUID string) *FetchRequestBuilder {
	b.topicUUID = topicUUID
	return b
}

func (b *FetchRequestBuilder) WithPartitionID(partitionID int32) *FetchRequestBuilder {
	b.partitionID = partitionID
	return b
}

func (b *FetchRequestBuilder) Build() kafkaapi.FetchRequest {
	// We have not designed stages for fetching from multiple topics/partitions
	return kafkaapi.FetchRequest{
		Header: NewRequestHeaderBuilder().BuildFetchRequestHeader(b.correlationId),
		Body: kafkaapi.FetchRequestBody{
			MaxWaitMS: value.Int32{Value: 500},
			MinBytes:  value.Int32{Value: 1},
			MaxBytes:  value.Int32{Value: math.MaxInt32},
			SessionId: value.Int32{Value: b.sessionId},
			Topics: []kafkaapi.Topic{
				{
					UUID: value.UUID{Value: b.topicUUID},
					Partitions: []kafkaapi.Partition{
						{
							ID:                 value.Int32{Value: b.partitionID},
							CurrentLeaderEpoch: value.Int32{Value: -1},
							FetchOffset:        value.Int64{Value: 0},
							LastFetchedOffset:  value.Int32{Value: -1},
							LogStartOffset:     value.Int64{Value: -1},
							PartitionMaxBytes:  value.Int32{Value: math.MaxInt32},
						},
					},
				},
			},
			ForgottenTopics: []kafkaapi.ForgottenTopic{},
		},
	}
}
