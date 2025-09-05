package legacy_builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafkaapi/legacy_headers"
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
	if version != 0 && version != 1 {
		panic("CodeCrafters Internal Error: Version has to be >= 0 and <= 1")
	}

	rb.version = version
	return rb
}

func (rb *ResponseHeaderBuilder) Build() legacy_headers.ResponseHeader {
	if rb.correlationId == -1 {
		panic("CodeCrafters Internal Error: Correlation ID is required")
	}

	return legacy_headers.ResponseHeader{
		Version:       rb.version,
		CorrelationId: rb.correlationId,
	}
}

func BuildResponseHeader(correlationId int32) legacy_headers.ResponseHeader {
	return NewResponseHeaderBuilder().WithCorrelationId(correlationId).Build()
}

func buildEmptyResponseHeader(version int) legacy_headers.ResponseHeader {
	return legacy_headers.ResponseHeader{
		Version: version,
	}
}

func BuildEmptyResponseHeaderv0() legacy_headers.ResponseHeader {
	return buildEmptyResponseHeader(0)
}

func BuildEmptyResponseHeaderv1() legacy_headers.ResponseHeader {
	return buildEmptyResponseHeader(1)
}
