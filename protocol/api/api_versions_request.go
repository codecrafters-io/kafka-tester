package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type ApiVersionsRequestBody struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ClientSoftwareName contains the name of the client.
	ClientSoftwareName string
	// ClientSoftwareVersion contains the version of the client.
	ClientSoftwareVersion string
}

func (r ApiVersionsRequestBody) encode(enc *encoder.Encoder) {
	if r.Version < 3 {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported API version: %d", r.Version))
	}
	enc.PutCompactString(r.ClientSoftwareName)
	enc.PutCompactString(r.ClientSoftwareVersion)
	enc.PutEmptyTaggedFieldArray()
}

func (r ApiVersionsRequestBody) Encode() []byte {
	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 4096))

	r.encode(&encoder)

	return encoder.ToBytes()
}

type ApiVersionsRequest struct {
	Header headers.RequestHeader
	Body   ApiVersionsRequestBody
}

func (r ApiVersionsRequest) Encode() []byte {
	return encoder.PackMessage(append(r.Header.Encode(), r.Body.Encode()...))
}
