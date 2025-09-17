package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
)

type ForgottenTopic struct {
	UUID       string
	Partitions []int32
}

func (f ForgottenTopic) Encode() []byte {
	topicEncoder := encoder.NewEncoder()
	topicEncoder.WriteUUID(f.UUID)
	topicEncoder.WriteCompactArrayOfInt32(f.Partitions)
	return topicEncoder.Bytes()
}

type FetchRequestBody struct {
	MaxWaitMS       int32
	MinBytes        int32
	MaxBytes        int32
	IsolationLevel  int8
	SessionId       int32
	SessionEpoch    int32
	Topics          []Topic
	ForgottenTopics []ForgottenTopic
	RackID          string
}

func (r FetchRequestBody) Encode() []byte {
	bodyEncoder := encoder.NewEncoder()
	bodyEncoder.WriteInt32(r.MaxWaitMS)
	bodyEncoder.WriteInt32(r.MinBytes)
	bodyEncoder.WriteInt32(r.MaxBytes)
	bodyEncoder.WriteInt8(r.IsolationLevel)
	bodyEncoder.WriteInt32(r.SessionId)
	bodyEncoder.WriteInt32(r.SessionEpoch)

	// encode topics
	bodyEncoder.WriteCompactArrayLength(len(r.Topics))
	for _, topic := range r.Topics {
		topic.Encode(bodyEncoder)
	}

	// encode forgotten topics
	bodyEncoder.WriteCompactArrayLength(len(r.ForgottenTopics))
	for _, forgottenTopic := range r.ForgottenTopics {
		bodyEncoder.WriteRawBytes(forgottenTopic.Encode())
	}

	bodyEncoder.WriteCompactString(r.RackID)

	bodyEncoder.WriteEmptyTagBuffer()
	return bodyEncoder.Bytes()
}

type FetchRequest struct {
	Header headers.RequestHeader
	Body   FetchRequestBody
}

// GetHeader implements the RequestI interface
func (r FetchRequest) GetHeader() headers.RequestHeader {
	return r.Header
}

// GetEncodedBody implements the RequestI interface
func (r FetchRequest) GetEncodedBody() []byte {
	return r.Body.Encode()
}
