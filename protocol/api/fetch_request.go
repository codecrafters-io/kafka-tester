package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type Partition struct {
	ID                 int32 // partition id
	CurrentLeaderEpoch int32 // current leader epoch
	FetchOffset        int64 // fetch offset
	LastFetchedOffset  int32 // last fetched offset
	LogStartOffset     int64 // log start offset
	PartitionMaxBytes  int32 // max bytes to fetch
}

func (p *Partition) Encode(pe *encoder.RealEncoder) {
	pe.PutInt32(p.ID)
	pe.PutInt32(p.CurrentLeaderEpoch)
	pe.PutInt64(p.FetchOffset)
	pe.PutInt32(p.LastFetchedOffset)
	pe.PutInt64(p.LogStartOffset)
	pe.PutInt32(p.PartitionMaxBytes)
	pe.PutEmptyTaggedFieldArray()
}

type Topic struct {
	TopicUUID  string
	Partitions []Partition
}

func (t *Topic) Encode(pe *encoder.RealEncoder) {
	uuidBytes, err := encoder.EncodeUUID(t.TopicUUID)
	if err != nil {
		return
	}
	pe.PutRawBytes(uuidBytes)

	// Encode partitions array length
	pe.PutCompactArrayLength(len(t.Partitions))

	// Encode each partition
	for _, partition := range t.Partitions {
		partition.Encode(pe)
	}

	pe.PutEmptyTaggedFieldArray()
}

type ForgottenTopic struct {
	TopicUUID  string
	Partitions []int32
}

func (f *ForgottenTopic) Encode(pe *encoder.RealEncoder) {
	uuidBytes, err := encoder.EncodeUUID(f.TopicUUID)
	if err != nil {
		return
	}
	pe.PutRawBytes(uuidBytes)

	pe.PutCompactInt32Array(f.Partitions)
}

type FetchRequestBody struct {
	MaxWaitMS         int32
	MinBytes          int32
	MaxBytes          int32
	IsolationLevel    int8
	FetchSessionID    int32
	FetchSessionEpoch int32
	Topics            []Topic
	ForgottenTopics   []ForgottenTopic
	RackID            string
}

func (r *FetchRequestBody) Encode(pe *encoder.RealEncoder) {
	pe.PutInt32(r.MaxWaitMS)
	pe.PutInt32(r.MinBytes)
	pe.PutInt32(r.MaxBytes)
	pe.PutInt8(r.IsolationLevel)
	pe.PutInt32(r.FetchSessionID)
	pe.PutInt32(r.FetchSessionEpoch)

	// Encode topics array length
	pe.PutCompactArrayLength(len(r.Topics))

	// Encode each topic
	for _, topic := range r.Topics {
		topic.Encode(pe)
	}

	// Encode forgotten topics array length
	pe.PutCompactArrayLength(len(r.ForgottenTopics))

	// Encode each forgotten topic
	for _, forgottenTopic := range r.ForgottenTopics {
		forgottenTopic.Encode(pe)
	}

	pe.PutCompactString(r.RackID)

	pe.PutEmptyTaggedFieldArray()
}

type FetchRequest struct {
	Header RequestHeader
	Body   FetchRequestBody
}

func (r FetchRequest) Encode() []byte {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	r.Header.Encode(&encoder)
	r.Body.Encode(&encoder)
	message := encoder.PackMessage()

	return message
}

func (r FetchRequest) GetHeader() RequestHeader {
	return r.Header
}
