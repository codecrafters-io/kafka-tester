package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

func encodeRequest(req builder.RequestI) []byte {
	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 4096))

	req.GetHeader().Encode(&encoder)
	req.GetBody().Encode(&encoder)

	return encoder.PackMessage()
}
