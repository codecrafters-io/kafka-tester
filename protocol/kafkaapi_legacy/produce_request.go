package kafkaapi_legacy

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy/headers_legacy"
)

type ProducePartitionData struct {
	Index         int32         // partition index
	RecordBatches []RecordBatch // record data to be produced.
}

func (p ProducePartitionData) encode(pe *encoder_legacy.Encoder) {
	pe.PutInt32(p.Index)
	pe.PutCompactArrayLength(RecordBatches(p.RecordBatches).getEncodedLength())
	for _, recordBatch := range p.RecordBatches {
		recordBatch.Encode(pe)
	}
	pe.PutEmptyTaggedFieldArray()
}

type ProduceTopicData struct {
	Name       string                 // topic name
	Partitions []ProducePartitionData // each partition to produce to
}

func (t ProduceTopicData) encode(pe *encoder_legacy.Encoder) {
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
	Topics    []ProduceTopicData // topics to produce to
}

func (r ProduceRequestBody) encode(pe *encoder_legacy.Encoder) {
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
	encoder := encoder_legacy.Encoder{}
	encoder.Init(make([]byte, 4096))
	r.encode(&encoder)
	return encoder.ToBytes()
}

type ProduceRequest struct {
	Header headers_legacy.RequestHeader
	Body   ProduceRequestBody
}

func (r ProduceRequest) GetHeader() headers_legacy.RequestHeader {
	return r.Header
}

func (r ProduceRequest) Encode() []byte {
	return encoder_legacy.PackMessage(append(r.Header.Encode(), r.Body.Encode()...))
}
