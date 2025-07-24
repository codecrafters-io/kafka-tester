package builder

import (
	headers "github.com/codecrafters-io/kafka-tester/protocol/api/headers"
)

type ResponseHeaderBuilder struct {
	correlationId int32
}

func NewResponseHeaderBuilder() *ResponseHeaderBuilder {
	return &ResponseHeaderBuilder{
		correlationId: -1,
	}
}

func (rb *ResponseHeaderBuilder) WithCorrelationId(correlationId int32) *ResponseHeaderBuilder {
	rb.correlationId = correlationId
	return rb
}

func (rb *ResponseHeaderBuilder) Build() headers.ResponseHeader {
	if rb.correlationId == -1 {
		panic("CodeCrafters Internal Error: correlationId can't be -1 for building a response header")
	}

	return headers.ResponseHeader{
		CorrelationId: rb.correlationId,
	}
}
