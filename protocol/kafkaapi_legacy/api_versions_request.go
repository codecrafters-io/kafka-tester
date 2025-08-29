package kafkaapi_legacy

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy/headers_legacy"
)

type ApiVersionsRequestBody struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ClientSoftwareName contains the name of the client.
	ClientSoftwareName string
	// ClientSoftwareVersion contains the version of the client.
	ClientSoftwareVersion string
}

func (r ApiVersionsRequestBody) encode(enc *encoder_legacy.Encoder) {
	if r.Version < 3 {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported API version: %d", r.Version))
	}
	enc.PutCompactString(r.ClientSoftwareName)
	enc.PutCompactString(r.ClientSoftwareVersion)
	enc.PutEmptyTaggedFieldArray()
}

func (r ApiVersionsRequestBody) Encode() []byte {
	encoder := encoder_legacy.Encoder{}
	encoder.Init(make([]byte, 4096))

	r.encode(&encoder)

	return encoder.ToBytes()
}

type ApiVersionsRequest struct {
	Header headers_legacy.RequestHeader
	Body   ApiVersionsRequestBody
}

func (r ApiVersionsRequest) GetHeader() headers_legacy.RequestHeader {
	return r.Header
}

func (r ApiVersionsRequest) Encode() []byte {
	return encoder_legacy.PackMessage(append(r.Header.Encode(), r.Body.Encode()...))
}
