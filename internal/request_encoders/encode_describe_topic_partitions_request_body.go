package request_encoders

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

func encodeDescribeTopicPartitionsRequestBody(requestBody kafkaapi.DescribeTopicPartitionsRequestBody, encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Body")
	defer encoder.PopPathContext()

	encodeCompactArray(requestBody.TopicNames, encoder, "Topics", encodeDescribeTopicPartitionRequestTopic)

	encoder.WriteInt32Field("ResponsePartitionLimit", requestBody.ResponsePartitionLimit)
	encodeCursor(requestBody.Cursor, encoder)
	encoder.WriteEmptyTagBuffer()
}

func encodeDescribeTopicPartitionRequestTopic(topicName value.CompactString, encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Topic")
	defer encoder.PopPathContext()
	encoder.WriteCompactStringField("Name", topicName)
	encoder.WriteEmptyTagBuffer()
}

func encodeCursor(cursor *kafkaapi.Cursor, encoder *field_encoder.FieldEncoder) {
	if cursor != nil {
		panic("Codecrafters Internal Error - Cursor is non-nil in DescribeTopicPartitionsRequestBody")
	}

	encoder.PushPathContext("Cursor")
	encoder.WriteInt8Field("IsCursorPresent", value.Int8{Value: -1})
	encoder.PopPathContext()
}
