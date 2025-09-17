package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
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

func (r ApiVersionsRequestBody) Encode(encoder *field_encoder.FieldEncoder) {
	if r.Version < 4 {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported API version: %d", r.Version))
	}
	encoder.PushPathContext("Body")
	defer encoder.PopPathContext()
	encoder.WriteCompactString("ClientSoftwareName", r.ClientSoftwareName)
	encoder.WriteCompactString("ClientSoftwareVersion", r.ClientSoftwareVersion)
	encoder.WriteEmptyTagBuffer()
}

type ApiVersionsRequest struct {
	Header headers.RequestHeader
	Body   ApiVersionsRequestBody
}

// GetHeader implements the RequestI interface
func (r ApiVersionsRequest) GetHeader() headers.RequestHeader {
	return r.Header
}

// EncodeBody implements the RequestI interface
func (r ApiVersionsRequest) EncodeBody(encoder *field_encoder.FieldEncoder) {
	r.Body.Encode(encoder)
}
