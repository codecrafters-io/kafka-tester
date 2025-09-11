package kafkaapi

import (
	"fmt"

	protocol_encoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
)

type ApiVersionsRequestBody struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ClientSoftwareName contains the name of the client.
	ClientSoftwareName string
	// ClientSoftwareVersion contains the version of the client.
	ClientSoftwareVersion string
}

func (r ApiVersionsRequestBody) Encode() []byte {
	if r.Version < 4 {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported API version: %d", r.Version))
	}

	encoder := protocol_encoder.NewEncoder()
	encoder.WriteCompactString(r.ClientSoftwareName)
	encoder.WriteCompactString(r.ClientSoftwareVersion)
	encoder.WriteEmptyTagBuffer()
	return encoder.Bytes()
}

type ApiVersionsRequest struct {
	Header headers.RequestHeader
	Body   ApiVersionsRequestBody
}

// GetHeader implements the RequestI interface
func (r ApiVersionsRequest) GetHeader() headers.RequestHeader {
	return r.Header
}

// GetEncodedBody implements the RequestI interface
func (r ApiVersionsRequest) GetEncodedBody() []byte {
	return r.Body.Encode()
}
