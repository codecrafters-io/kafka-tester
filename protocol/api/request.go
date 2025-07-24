package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	kafka_interface "github.com/codecrafters-io/kafka-tester/protocol/interface"
)

func encodeRequest(req kafka_interface.RequestI) []byte {
	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 4096))

	req.GetHeader().Encode(&encoder)
	req.GetBody().Encode(&encoder)

	return encoder.PackMessage()
}
