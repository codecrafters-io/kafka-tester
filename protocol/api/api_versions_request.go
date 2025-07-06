package kafkaapi

import (
	realencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type ApiVersionsRequestBody struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ClientSoftwareName contains the name of the client.
	ClientSoftwareName string
	// ClientSoftwareVersion contains the version of the client.
	ClientSoftwareVersion string
}

func (r *ApiVersionsRequestBody) Encode(enc *realencoder.RealEncoder) {
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

func (r *ApiVersionsRequest) Encode() []byte {
	encoder := realencoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	r.Header.EncodeV2(&encoder)
	r.Body.Encode(&encoder)
	messageBytes := encoder.PackMessage()

	return messageBytes
}
