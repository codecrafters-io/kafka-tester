package kafkaapi

import (
	headers "github.com/codecrafters-io/kafka-tester/protocol/api/headers"
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
	Header headers.RequestHeader
	Body   ApiVersionsRequestBody
}

func (r ApiVersionsRequest) Encode() []byte {
	return encodeRequest(r)
}

func (r ApiVersionsRequest) GetHeader() headers.RequestHeader {
	return r.Header
}

func (r ApiVersionsRequest) GetBody() builder.RequestBodyI {
	return r.Body
}
