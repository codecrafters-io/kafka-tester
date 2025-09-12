package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
)

type Cursor struct {
	TopicName      string
	PartitionIndex int32
}

func (c Cursor) Encode() []byte {
	cursorEncoder := encoder.NewEncoder()

	cursorEncoder.WriteCompactString(c.TopicName)
	cursorEncoder.WriteInt32(c.PartitionIndex)
	cursorEncoder.WriteEmptyTagBuffer()
	return cursorEncoder.Bytes()
}

type DescribeTopicPartitionsRequestBody struct {
	TopicNames             []string
	ResponsePartitionLimit int32
	// This is unused because we don't test using cursors in this extension
	Cursor *Cursor
}

func (r DescribeTopicPartitionsRequestBody) Encode() []byte {
	bodyEncoder := encoder.NewEncoder()

	bodyEncoder.WriteCompactArrayLength(len(r.TopicNames))
	for _, topicName := range r.TopicNames {
		bodyEncoder.WriteCompactString(topicName)
		bodyEncoder.WriteEmptyTagBuffer()
	}

	bodyEncoder.WriteInt32(r.ResponsePartitionLimit)

	if r.Cursor == nil {
		bodyEncoder.WriteInt8(-1)
	} else {
		bodyEncoder.WriteRawBytes(r.Cursor.Encode())
	}

	bodyEncoder.WriteEmptyTagBuffer()
	return bodyEncoder.Bytes()
}

type DescribeTopicPartitionsRequest struct {
	Header headers.RequestHeader
	Body   DescribeTopicPartitionsRequestBody
}

// GetHeader implements the RequestI interface
func (r DescribeTopicPartitionsRequest) GetHeader() headers.RequestHeader {
	return r.Header
}

// GetEncodedBody implements the RequestI interface
func (r DescribeTopicPartitionsRequest) GetEncodedBody() []byte {
	return r.Body.Encode()
}
