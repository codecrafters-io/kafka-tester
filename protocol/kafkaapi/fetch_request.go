package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ForgottenTopic struct {
	UUID         value.UUID
	PartitionIds []value.Int32
}

type FetchRequestBody struct {
	MaxWaitMS       value.Int32
	MinBytes        value.Int32
	MaxBytes        value.Int32
	IsolationLevel  value.Int8
	SessionId       value.Int32
	SessionEpoch    value.Int32
	Topics          []Topic
	ForgottenTopics []ForgottenTopic
	RackId          value.CompactString
}

type FetchRequest struct {
	Header headers.RequestHeader
	Body   FetchRequestBody
}

// GetHeader implements the RequestI interface
func (r FetchRequest) GetHeader() headers.RequestHeader {
	return r.Header
}
