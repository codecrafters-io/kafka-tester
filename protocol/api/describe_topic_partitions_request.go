package kafkaapi

import (
	headers "github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	kafka_interface "github.com/codecrafters-io/kafka-tester/protocol/interface"
)

type DescribeTopicPartitionsRequestBody struct {
	Topics                 []TopicName
	ResponsePartitionLimit int32
	Cursor                 Cursor
}

func (r DescribeTopicPartitionsRequestBody) Encode(pe *encoder.Encoder) {
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

func (r DescribeTopicPartitionsRequestBody) Encode() []byte {
	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 4096))

	r.encode(&encoder)

	return encoder.ToBytes()
}

type TopicName struct {
	Name string
}

func (t TopicName) Encode(pe *encoder.Encoder) {
	pe.PutCompactString(t.Name)
	pe.PutEmptyTaggedFieldArray()
}

type Cursor struct {
	TopicName      string
	PartitionIndex int32
}

func (c Cursor) Encode(pe *encoder.Encoder) {
	pe.PutCompactString(c.TopicName)
	pe.PutInt32(c.PartitionIndex)
	pe.PutEmptyTaggedFieldArray()
}

type DescribeTopicPartitionsRequest struct {
	Header headers.RequestHeader
	Body   DescribeTopicPartitionsRequestBody
}

func (r DescribeTopicPartitionsRequest) Encode() []byte {
	return encodeRequest(r)
}

func (r DescribeTopicPartitionsRequest) GetHeader() headers.RequestHeader {
	return r.Header
}

func (r DescribeTopicPartitionsRequest) GetBody() kafka_interface.RequestBodyI {
	return r.Body
}
