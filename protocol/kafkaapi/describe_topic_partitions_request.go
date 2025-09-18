package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Cursor struct {
	TopicName      value.CompactString
	PartitionIndex value.Int32
}

type DescribeTopicPartitionsRequestBody struct {
	TopicNames             []value.CompactString
	ResponsePartitionLimit value.Int32
	// This is unused because we don't test using cursors in this extension
	Cursor *Cursor
}

type DescribeTopicPartitionsRequest struct {
	Header headers.RequestHeader
	Body   DescribeTopicPartitionsRequestBody
}

// GetHeader implements the RequestI interface
func (r DescribeTopicPartitionsRequest) GetHeader() headers.RequestHeader {
	return r.Header
}
