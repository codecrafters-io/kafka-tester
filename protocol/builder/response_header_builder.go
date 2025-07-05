package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ResponseHeaderBuilder struct {
	correlationId int32
}

func NewResponseHeaderBuilder() *ResponseHeaderBuilder {
	return &ResponseHeaderBuilder{}
}

func (rb *ResponseHeaderBuilder) WithCorrelationId(correlationId int32) *ResponseHeaderBuilder {
	rb.correlationId = correlationId
	return rb
}

func (rb *ResponseHeaderBuilder) Build() kafkaapi.ResponseHeader {
	if rb.correlationId == 0 {
		panic("CodeCrafters Internal Error: correlationId is required for building a response header")
	}

	return kafkaapi.ResponseHeader{
		CorrelationId: rb.correlationId,
	}
}
