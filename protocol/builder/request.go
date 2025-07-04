package builder

import "github.com/codecrafters-io/kafka-tester/protocol/encoder"

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
