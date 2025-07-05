package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type RequestHeaderI interface {
	Encode(*encoder.RealEncoder)
}

type RequestBodyI interface {
	Encode(*encoder.RealEncoder)
}

type KafkaRequestI interface {
	GetHeader() RequestHeaderI
	GetBody() RequestBodyI
}

type ResponseBuilderI interface {
	// TODO: ResponseI
	Build(correlationId int32) kafkaapi.ProduceResponse
}

type ResponseHeaderBuilderI interface {
	Build() kafkaapi.ResponseHeader
}

type RequestBuilderI interface {
	AddRecordBatchToTopicPartition(topicName string, partitionIndex int32, messages []string) *ProduceRequestBuilder
	Build() RequestBodyI
	BuildProduceRequest() kafkaapi.ProduceRequestBody
}
