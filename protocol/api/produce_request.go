package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

// PartitionData : v11 (No v12 Response present, v11 Request is identical to v12)
type PartitionData struct {
	Index   int32         // partition index
	Records []RecordBatch // record data to be produced.
	// TODO: Record or RecordBatch?
}

func (p *PartitionData) Encode(pe *encoder.RealEncoder) {
	pe.PutInt32(p.Index)
	pe.PutCompactArrayLength(len(p.Records))
	for _, recordBatch := range p.Records {
		recordBatch.Encode(pe)
	}
	pe.PutEmptyTaggedFieldArray()
}

type TopicData struct {
	Name       string          // topic name
	Partitions []PartitionData // each partition to produce to
}

func (t *TopicData) Encode(pe *encoder.RealEncoder) {
	pe.PutString(t.Name)
	pe.PutCompactArrayLength(len(t.Partitions))
	for _, partition := range t.Partitions {
		partition.Encode(pe)
	}
	pe.PutEmptyTaggedFieldArray()
}

type ProduceRequestBody struct {
	TransactionalID string // transactional id or null if not transactional
	// NoResponse: 0
	// WaitForLocal: local commit only: 1
	// WaitForAll: all in-sync replicas: -1
	Acks      int16 // number of acks the producer requires the leader to have received before considering a request complete
	TimeoutMs int32
	Topics    []TopicData // topics to produce to
}

func (pr *ProduceRequestBody) Encode(pe *encoder.RealEncoder) {
	pe.PutNullableCompactString(&pr.TransactionalID)
	pe.PutInt16(int16(pr.Acks))
	pe.PutInt32(pr.TimeoutMs)
	pe.PutArrayLength(len(pr.Topics))
	for _, topic := range pr.Topics {
		topic.Encode(pe)
	}
	pe.PutEmptyTaggedFieldArray()
}

type ProduceRequest struct {
	Header RequestHeader
	Body   ProduceRequestBody
}
