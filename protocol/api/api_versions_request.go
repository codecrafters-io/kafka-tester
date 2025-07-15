package kafkaapi

import (
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

func (r ApiVersionsRequestBody) Encode(enc *encoder.RealEncoder) {
	if r.Version >= 3 {
		enc.PutCompactString(r.ClientSoftwareName)
		enc.PutCompactString(r.ClientSoftwareVersion)
		enc.PutEmptyTaggedFieldArray()
	}
}

type ApiVersionsRequest struct {
	Header RequestHeader
	Body   ApiVersionsRequestBody
}

func (r ApiVersionsRequest) Encode() []byte {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	r.Header.Encode(&encoder)
	r.Body.Encode(&encoder)
	messageBytes := encoder.PackMessage()

	return messageBytes
}

func (r ApiVersionsRequest) GetApiType() string {
	return "ApiVersions"
}

func (r ApiVersionsRequest) GetApiVersion() int16 {
	return r.Header.ApiVersion
}

func (r ApiVersionsRequest) GetCorrelationId() int32 {
	return r.Header.CorrelationId
}
