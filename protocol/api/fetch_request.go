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

func (p *Partition) Encode(enc *encoder.RealEncoder) {
	enc.PutInt32(p.ID)
	enc.PutInt32(p.CurrentLeaderEpoch)
	enc.PutInt64(p.FetchOffset)
	enc.PutInt32(p.LastFetchedOffset)
	enc.PutInt64(p.LogStartOffset)
	enc.PutInt32(p.PartitionMaxBytes)
	enc.PutEmptyTaggedFieldArray()
}

type Topic struct {
	TopicUUID  string
	Partitions []Partition
}

func (t *Topic) Encode(enc *encoder.RealEncoder) {
	uuidBytes, err := encoder.EncodeUUID(t.TopicUUID)
	if err != nil {
		return
	}
	if err := enc.PutRawBytes(uuidBytes); err != nil {
		return
	}

	// Encode partitions array length
	enc.PutCompactArrayLength(len(t.Partitions))

	// Encode each partition
	for _, partition := range t.Partitions {
		partition.Encode(enc)
	}
}

type ForgottenTopic struct {
	TopicUUID  string
	Partitions []int32
}

func (f *ForgottenTopic) Encode(enc *encoder.RealEncoder) {
	uuidBytes, err := encoder.EncodeUUID(f.TopicUUID)
	if err != nil {
		return
	}
	if err := enc.PutRawBytes(uuidBytes); err != nil {
		return
	}

	enc.PutCompactInt32Array(f.Partitions)
}

type FetchRequest struct {
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

func (r *FetchRequest) Encode(enc *encoder.RealEncoder) {
	enc.PutInt32(r.MaxWaitMS)
	enc.PutInt32(r.MinBytes)
	enc.PutInt32(r.MaxBytes)
	enc.PutInt8(r.IsolationLevel)
	enc.PutInt32(r.FetchSessionID)
	enc.PutInt32(r.FetchSessionEpoch)

	// Encode topics array length
	enc.PutCompactArrayLength(len(r.Topics))

	// Encode each topic
	for _, topic := range r.Topics {
		topic.Encode(enc)
	}

	// Encode forgotten topics array length
	enc.PutCompactArrayLength(len(r.ForgottenTopics))

	// Encode each forgotten topic
	for _, forgottenTopic := range r.ForgottenTopics {
		forgottenTopic.Encode(enc)
	}

	enc.PutCompactString(r.RackID)

	enc.PutEmptyTaggedFieldArray()
}
