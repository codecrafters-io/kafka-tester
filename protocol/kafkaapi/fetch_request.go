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

	encoder.WriteUUIDField("UUID", f.UUID)
	f.encodePartitions(encoder)
}

func (f ForgottenTopic) encodePartitions(encoder *field_encoder.FieldEncoder) {
	// WIP
	encoder.WriteCompactArrayLengthField()
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
	encodableTopics := make([]FieldEncodable, len(r.Topics))
	for i, topic := range r.Topics {
		encodableTopics[i] = topic
	}
	encodeCompactArray("Topics", encoder, encodableTopics)
}

func (r FetchRequestBody) encodeForgottenTopics(encoder *field_encoder.FieldEncoder) {
	encodableForgottenTopics := make([]FieldEncodable, len(r.Topics))
	for i, topic := range r.Topics {
		encodableForgottenTopics[i] = topic
	}
	encodeCompactArray("ForgottenTopics", encoder, encodableForgottenTopics)
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
