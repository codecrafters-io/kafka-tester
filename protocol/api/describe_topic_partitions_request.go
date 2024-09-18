package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type TopicName struct {
	Name string
}

func (t *TopicName) Encode(pe *encoder.RealEncoder) {
	pe.PutCompactString(t.Name)
	// pe.PutEmptyTaggedFieldArray()
}

func (t *TopicName) Decode(decoder *decoder.RealDecoder) error {
	name, err := decoder.GetCompactString()
	if err != nil {
		return err
	}
	t.Name = name

	return nil
}

type Cursor struct {
	TopicName      string
	PartitionIndex int32
}

func (c *Cursor) Decode(decoder *decoder.RealDecoder) error {
	topicName, err := decoder.GetCompactString()
	if err != nil {
		return err
	}
	c.TopicName = topicName

	partitionIndex, err := decoder.GetInt8()
	if err != nil {
		return err
	}
	c.PartitionIndex = int32(partitionIndex)

	return nil
}

func (c *Cursor) Encode(pe *encoder.RealEncoder) {
	pe.PutCompactString(c.TopicName)
	// pe.PutInt32(c.PartitionIndex)
	pe.PutInt8(int8(c.PartitionIndex))
	// pe.PutEmptyTaggedFieldArray()
}

type DescribeTopicPartitionRequest struct {
	Header RequestHeader
	Body   DescribeTopicPartitionRequestBody
}

type DescribeTopicPartitionRequestBody struct {
	Topics                 []TopicName
	ResponsePartitionLimit int32
	Cursor                 Cursor
}

func (r *DescribeTopicPartitionRequestBody) Encode(pe *encoder.RealEncoder) {
	// Encode topics array length
	pe.PutCompactArrayLength(len(r.Topics))

	// Encode each topic
	for _, topic := range r.Topics {
		topic.Encode(pe)
	}

	pe.PutInt32(r.ResponsePartitionLimit)

	r.Cursor.Encode(pe)

	pe.PutEmptyTaggedFieldArray()
}

func (r *DescribeTopicPartitionRequestBody) Decode(decoder *decoder.RealDecoder) error {
	// Decode topics array length
	topicsLength, err := decoder.GetCompactArrayLength()
	if err != nil {
		return err
	}

	fmt.Printf("topicsLength: %d\n", topicsLength)

	// Decode each topic
	for i := 0; i < topicsLength; i++ {
		topic := TopicName{}
		err = topic.Decode(decoder)
		if err != nil {
			return err
		}
		fmt.Printf("topic: %+v\n", topic)
		r.Topics = append(r.Topics, topic)
	}

	r.ResponsePartitionLimit, err = decoder.GetInt32()
	if err != nil {
		return err
	}

	fmt.Printf("Remaining: %d\n", decoder.Remaining())

	err = r.Cursor.Decode(decoder)
	if err != nil {
		return err
	}

	decoder.GetEmptyTaggedFieldArray()

	fmt.Printf("Remaining: %d\n", decoder.Remaining())

	return nil
}
