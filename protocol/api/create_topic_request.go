package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type ConfigData struct {
	Name  string
	Value string
}

func (c *ConfigData) Encode(pe *encoder.Encoder) {
	pe.PutCompactString(c.Name)
	pe.PutNullableCompactString(&c.Value)
	pe.PutEmptyTaggedFieldArray()
}

type AssignmentData struct {
	PartitionIndex int32
	BrokerIds      []int32
}

func (a *AssignmentData) Encode(pe *encoder.Encoder) {
	pe.PutInt32(a.PartitionIndex)
	pe.PutCompactArrayLength(len(a.BrokerIds))
	pe.PutNullableCompactInt32Array(a.BrokerIds)
	pe.PutEmptyTaggedFieldArray()
}

type TopicData struct {
	Name              string // topic name
	NumPartitions     int32  // number of partitions
	ReplicationFactor int16  // replication factor
	Assignments       []AssignmentData
	Configs           []ConfigData
}

func (t *TopicData) Encode(pe *encoder.Encoder) {
	pe.PutCompactString(t.Name)
	pe.PutInt32(t.NumPartitions)
	pe.PutInt16(t.ReplicationFactor)
	pe.PutCompactArrayLength(len(t.Assignments))
	for _, assignment := range t.Assignments {
		assignment.Encode(pe)
	}
	pe.PutCompactArrayLength(len(t.Configs))
	for _, config := range t.Configs {
		config.Encode(pe)
	}
	pe.PutEmptyTaggedFieldArray()
}

type CreateTopicRequestBody struct {
	Topics       []TopicData // topics to create
	TimeoutMs    int32       // timeout in milliseconds
	ValidateOnly bool        // validate only
}

func (pr *CreateTopicRequestBody) Encode(pe *encoder.Encoder) {
	pe.PutCompactArrayLength(len(pr.Topics))
	for _, topic := range pr.Topics {
		topic.Encode(pe)
	}
	pe.PutInt32(pr.TimeoutMs)
	pe.PutBool(pr.ValidateOnly)
	pe.PutEmptyTaggedFieldArray()
}

type CreateTopicRequest struct {
	Header RequestHeader
	Body   CreateTopicRequestBody
}

func (r *CreateTopicRequest) Encode(pe *encoder.Encoder) {
	r.Header.EncodeV2(pe)
	r.Body.Encode(pe)
}
