package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
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

func (r ApiVersionsRequestBody) Encode(enc *encoder.Encoder) {
	if r.Version < 3 {
		panic("CodeCrafers Internal Error: ApiVersionsRequestBody.Version must be >= 3")
	}

	enc.PutCompactString(r.ClientSoftwareName)
	enc.PutCompactString(r.ClientSoftwareVersion)
	enc.PutEmptyTaggedFieldArray()
}

type ApiVersionsRequest struct {
	Header RequestHeader
	Body   ApiVersionsRequestBody
}

func (r ApiVersionsRequest) Encode() []byte {
	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 4096))

	r.Header.Encode(&encoder)
	r.Body.Encode(&encoder)
	messageBytes := encoder.PackMessage()

	return messageBytes
}

func (r ApiVersionsRequest) GetHeader() builder.RequestHeaderI {
	return r.Header
}

func (r ApiVersionsRequest) GetBody() builder.RequestBodyI {
	return r.Body
}
