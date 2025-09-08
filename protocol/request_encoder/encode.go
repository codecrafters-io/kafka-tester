package request_encoder

import (
	protocol_encoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_interface"
)

func Encode(request kafka_interface.RequestI) []byte {
	return protocol_encoder.PackEncodedBytesAsMessage(append(request.GetHeader().Encode(), request.GetEncodedBody()...))
}
