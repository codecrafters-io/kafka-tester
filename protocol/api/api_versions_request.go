package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type ApiVersionsRequest struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ClientSoftwareName contains the name of the client.
	ClientSoftwareName string
	// ClientSoftwareVersion contains the version of the client.
	ClientSoftwareVersion string
}

func (r *ApiVersionsRequest) Encode(enc *encoder.RealEncoder) {
	if r.Version >= 3 {
		enc.PutEmptyTaggedFieldArray()
		enc.PutCompactString(r.ClientSoftwareName)
		enc.PutCompactString(r.ClientSoftwareVersion)
		enc.PutEmptyTaggedFieldArray()
	}
}
