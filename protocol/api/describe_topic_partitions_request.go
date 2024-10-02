package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type DescribeTopicPartitionsRequest struct {
	Header RequestHeader
	Body   DescribeTopicPartitionsRequestBody
}

func (r *DescribeTopicPartitionsRequest) Encode(pe *encoder.RealEncoder) {
	r.Header.EncodeV2(pe)
	r.Body.Encode(pe)
}

type DescribeTopicPartitionsRequestBody struct {
	Topics                 []TopicName
	ResponsePartitionLimit int32
	Cursor                 Cursor
}

func (r *DescribeTopicPartitionsRequestBody) Encode(pe *encoder.RealEncoder) {
	// Encode topics array length
	pe.PutCompactArrayLength(len(r.Topics))

	// Encode each topic
	for _, topic := range r.Topics {
		topic.Encode(pe)
	}

	pe.PutInt32(r.ResponsePartitionLimit)

	if r.Cursor.TopicName == "" && r.Cursor.PartitionIndex == 0 {
		// Cursor is nullable, if we don't have anything in the request to encode
		// just send -1
		pe.PutInt8(-1) // null
	} else {
		r.Cursor.Encode(pe)
	}

	pe.PutEmptyTaggedFieldArray()
}

type TopicName struct {
	Name string
}

func (t *TopicName) Encode(pe *encoder.RealEncoder) {
	pe.PutCompactString(t.Name)
	pe.PutEmptyTaggedFieldArray()
}

type Cursor struct {
	TopicName      string
	PartitionIndex int32
}

func (c *Cursor) Encode(pe *encoder.RealEncoder) {
	pe.PutCompactString(c.TopicName)
	pe.PutInt32(c.PartitionIndex)
	pe.PutEmptyTaggedFieldArray()
}
