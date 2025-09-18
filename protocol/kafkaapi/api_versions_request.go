package kafkaapi

import (
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
type ApiVersionsRequest struct {
	Header headers.RequestHeader
	Body   ApiVersionsRequestBody
}

// GetHeader implements the RequestI interface
func (r ApiVersionsRequest) GetHeader() headers.RequestHeader {
	return r.Header
}
