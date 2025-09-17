package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ApiVersionsRequestBody struct {
	// Version defines the protocol version to use for encode and decode
	Version value.Int16
	// ClientSoftwareName contains the name of the client.
	ClientSoftwareName value.CompactString
	// ClientSoftwareVersion contains the version of the client.
	ClientSoftwareVersion value.CompactString
}

func (r ApiVersionsRequestBody) Encode(encoder *field_encoder.FieldEncoder) {
	if r.Version.Value < 4 {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported API version: %d", r.Version))
	}
	encoder.PushPathContext("Body")
	defer encoder.PopPathContext()
	encoder.WriteCompactStringField("ClientSoftwareName", r.ClientSoftwareName)
	encoder.WriteCompactStringField("ClientSoftwareVersion", r.ClientSoftwareVersion)
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
