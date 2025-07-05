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
	BuildResponse(correlationId int32) kafkaapi.ProduceResponse
}

type ResponseHeaderBuilderI interface {
	Build() kafkaapi.ResponseHeader
}
