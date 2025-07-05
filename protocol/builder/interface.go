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

// type ResponseBuilderI interface {
// 	// TODO: ResponseI
// 	Build(correlationId int32) kafkaapi.ProduceResponse
// }

type ProduceResponseBuilderI interface {
	Build(correlationId int32) kafkaapi.ProduceResponse
}

type ApiVersionsResponseBuilderI interface {
	Build(correlationId int32) kafkaapi.ApiVersionsResponse
}

type ResponseHeaderBuilderI interface {
	Build() kafkaapi.ResponseHeader
}

// type RequestBuilderI interface {
// 	Build() RequestBodyI
// }

type ApiVersionsRequestBuilderI interface {
	Build() kafkaapi.ApiVersionsRequestBody
}

type ProduceRequestBuilderI interface {
	AddRecordBatchToTopicPartition(topicName string, partitionIndex int32, messages []string) ProduceRequestBuilderI
	Build() kafkaapi.ProduceRequestBody
}
