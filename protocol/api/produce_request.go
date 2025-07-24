package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type PartitionData struct {
	Index         int32         // partition index
	RecordBatches []RecordBatch // record data to be produced.
}

func (p PartitionData) encode(pe *encoder.Encoder) {
	pe.PutInt32(p.Index)
	pe.PutCompactArrayLength(RecordBatches(p.RecordBatches).getEncodedLength())
	for _, recordBatch := range p.RecordBatches {
		recordBatch.Encode(pe)
	}
	pe.PutEmptyTaggedFieldArray()
}

type TopicData struct {
	Name       string          // topic name
	Partitions []PartitionData // each partition to produce to
}

func (t TopicData) encode(pe *encoder.Encoder) {
	pe.PutCompactString(t.Name)
	pe.PutCompactArrayLength(len(t.Partitions))
	for _, partition := range t.Partitions {
		partition.encode(pe)
	}
	pe.PutEmptyTaggedFieldArray()
}

type ProduceRequestBody struct {
	TransactionalID *string // transactional id or null if not transactional
	// Acks: number of acks the producer requires the leader to have received
	// before considering a request complete
	// Possible values:
	// - NoResponse: 0
	// - WaitForLocal: local commit only: 1
	// - WaitForAll: all in-sync replicas: -1
	Acks      int16
	TimeoutMs int32
	Topics    []TopicData // topics to produce to
}

func (r ProduceRequestBody) encode(pe *encoder.Encoder) {
	pe.PutNullableCompactString(r.TransactionalID)
	pe.PutInt16(int16(r.Acks))
	pe.PutInt32(r.TimeoutMs)
	pe.PutCompactArrayLength(len(r.Topics))
	for _, topic := range r.Topics {
		topic.encode(pe)
	}
	pe.PutEmptyTaggedFieldArray()
}

func (r ProduceRequestBody) Encode() []byte {
	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 4096))
	r.encode(&encoder)
	return encoder.ToBytes()
}

type ProduceRequest struct {
	Header headers.RequestHeader
	Body   ProduceRequestBody
}

func (r ProduceRequest) GetHeader() headers.RequestHeader {
	return r.Header
}

func (r ProduceRequest) Encode() []byte {
	return encoder.PackMessage(append(r.Header.Encode(), r.Body.Encode()...))
}
