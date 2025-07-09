package builder

import kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"

type RequestI interface {
	Encode() []byte
	GetHeader() kafkaapi.RequestHeader
}

// TODO
//type Assertion struct {
// actualResponse kafkaResponseI
// expectedResponse kafkaResponseI
//	logger *logger.Logger
//	err    error
//}

type Assertion interface {
	Run() error
}
