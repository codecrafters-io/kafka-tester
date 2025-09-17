package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
)

type Cursor struct {
	TopicName      string
	PartitionIndex int32
}

func (c Cursor) Encode(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Cursor")
	defer encoder.PopPathContext()
	encoder.WriteCompactStringField("TopicName", c.TopicName)
	encoder.WriteInt32Field("PartitionIndex", c.PartitionIndex)
	encoder.WriteEmptyTagBuffer()
}

type DescribeTopicPartitionsRequestBody struct {
	TopicNames             []string
	ResponsePartitionLimit int32
	// This is unused because we don't test using cursors in this extension
	Cursor *Cursor
}

func (r DescribeTopicPartitionsRequestBody) Encode(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Body")
	defer encoder.PopPathContext()

	r.encodeTopics(encoder)
	encoder.WriteInt32Field("ResponsePartitionLimit", r.ResponsePartitionLimit)
	r.encodeCursor(encoder)

	encoder.WriteEmptyTagBuffer()
}

func (r DescribeTopicPartitionsRequestBody) encodeTopics(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Topics")
	defer encoder.PopPathContext()

	encoder.WriteCompactArrayLengthField("Length", len(r.TopicNames))
	for _, topicName := range r.TopicNames {
		encoder.WriteCompactStringField("Name", topicName)
		encoder.WriteEmptyTagBuffer()
	}
}

func (r DescribeTopicPartitionsRequestBody) encodeCursor(encoder *field_encoder.FieldEncoder) {
	if r.Cursor == nil {
		encoder.PushPathContext("Cursor")
		encoder.WriteInt8Field("IsCursorPresent", -1)
		encoder.PopPathContext()
	} else {
		r.Cursor.Encode(encoder)
	}
}

type DescribeTopicPartitionsRequest struct {
	Header headers.RequestHeader
	Body   DescribeTopicPartitionsRequestBody
}

// GetHeader implements the RequestI interface
func (r DescribeTopicPartitionsRequest) GetHeader() headers.RequestHeader {
	return r.Header
}

// EncodeBody implements the RequestI interface
func (r DescribeTopicPartitionsRequest) EncodeBody(encoder *field_encoder.FieldEncoder) {
	r.Body.Encode(encoder)
}
