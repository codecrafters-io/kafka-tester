package builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
)

type ResponseHeaderBuilder struct {
	correlationId int32
	version       int
}

func NewResponseHeaderBuilder() *ResponseHeaderBuilder {
	return &ResponseHeaderBuilder{
		version:       1,
		correlationId: -1,
	}
}

func (rb *ResponseHeaderBuilder) WithCorrelationId(correlationId int32) *ResponseHeaderBuilder {
	rb.correlationId = correlationId
	return rb
}

func (rb *ResponseHeaderBuilder) WithVersion(version int) *ResponseHeaderBuilder {
	rb.version = version
	return rb
}

func (rb *ResponseHeaderBuilder) Build() headers.ResponseHeader {
	if rb.correlationId == -1 {
		panic("CodeCrafters Internal Error: Correlation ID is required")
	}

	if rb.version != 0 && rb.version != 1 {
		panic("CodeCrafters Internal Error: Version has to be >= 0 and <= 1")
	}

	return headers.ResponseHeader{
		Version:       rb.version,
		CorrelationId: rb.correlationId,
	}
}

func BuildResponseHeader(correlationId int32) headers.ResponseHeader {
	return NewResponseHeaderBuilder().WithCorrelationId(correlationId).Build()
}

func BuildEmptyResponseHeader(version int) headers.ResponseHeader {
	return headers.ResponseHeader{
		Version: version,
	}
}
