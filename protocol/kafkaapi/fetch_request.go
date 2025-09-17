package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
)

type ForgottenTopic struct {
	UUID       string
	Partitions []int32
}

func (f ForgottenTopic) Encode(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("ForgottenTopic")
	defer encoder.PopPathContext()

	// topicEncoder.WriteUUID("UUID", f.UUID)
	// topicEncoder.WriteCompactArrayOfInt32("Partitions", f.Partitions)
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
	RackId          string
}

func (r FetchRequestBody) Encode(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Body")
	defer encoder.PopPathContext()

	encoder.WriteInt32Field("MaxWaitMS", r.MaxWaitMS)
	encoder.WriteInt32Field("MinBytes", r.MinBytes)
	encoder.WriteInt32Field("MaxBytes", r.MaxBytes)
	encoder.WriteInt8Field("IsolationLevel", r.IsolationLevel)
	encoder.WriteInt32Field("SessionID", r.SessionId)
	encoder.WriteInt32Field("SessionEpoch", r.SessionEpoch)

	// encode topics
	r.encodeTopics(encoder)
	r.encodeForgottenTopics(encoder)

	encoder.WriteCompactStringField("RackID", r.RackId)
	encoder.WriteEmptyTagBuffer()
}

func (r FetchRequestBody) encodeTopics(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Topics")
	defer encoder.PopPathContext()

	encoder.WriteCompactArrayLengthField("Length", len(r.Topics))
	for _, topic := range r.Topics {
		topic.Encode(encoder)
	}
}

func (r FetchRequestBody) encodeForgottenTopics(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Topics")
	defer encoder.PopPathContext()

	encoder.WriteCompactArrayLengthField("Length", len(r.ForgottenTopics))
	for _, forgottenTopic := range r.ForgottenTopics {
		forgottenTopic.Encode(encoder)
	}
}

type FetchRequest struct {
	Header headers.RequestHeader
	Body   FetchRequestBody
}

// GetHeader implements the RequestI interface
func (r FetchRequest) GetHeader() headers.RequestHeader {
	return r.Header
}

// EncodeBody implements the RequestI interface
func (r FetchRequest) EncodeBody(encoder *field_encoder.FieldEncoder) {
	r.Body.Encode(encoder)
}
