package builder

import kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"

type RequestI interface {
	Encode() []byte
	GetHeader() kafkaapi.RequestHeader
}

type Assertion interface {
	Run() error
}
