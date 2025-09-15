package builder

import (
	"math"

	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
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
			MaxWaitMS: 500,
			MinBytes:  1,
			MaxBytes:  math.MaxInt32,
			SessionId: b.sessionId,
			Topics: []kafkaapi.Topic{
				{
					UUID: b.topicUUID,
					Partitions: []kafkaapi.Partition{
						{
							ID:                 b.partitionID,
							CurrentLeaderEpoch: -1,
							FetchOffset:        0,
							LastFetchedOffset:  -1,
							LogStartOffset:     -1,
							PartitionMaxBytes:  math.MaxInt32,
						},
					},
				},
			},
			ForgottenTopics: []kafkaapi.ForgottenTopic{},
		},
	}
}
