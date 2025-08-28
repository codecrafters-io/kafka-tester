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
	if version != 0 && version != 1 {
		panic("CodeCrafters Internal Error: Version has to be >= 0 and <= 1")
	}

	rb.version = version
	return rb
}

func (rb *ResponseHeaderBuilder) Build() headers.ResponseHeader {
	if rb.correlationId == -1 {
		panic("CodeCrafters Internal Error: Correlation ID is required")
	}

	return headers.ResponseHeader{
		Version:       rb.version,
		CorrelationId: rb.correlationId,
	}
}

func BuildResponseHeader(correlationId int32) headers.ResponseHeader {
	return NewResponseHeaderBuilder().WithCorrelationId(correlationId).Build()
}

func buildEmptyResponseHeader(version int) headers.ResponseHeader {
	return headers.ResponseHeader{
		Version: version,
	}
}

func BuildEmptyResponseHeaderv0() headers.ResponseHeader {
	return buildEmptyResponseHeader(0)
}

func BuildEmptyResponseHeaderv1() headers.ResponseHeader {
	return buildEmptyResponseHeader(1)
}

/***************************** Updated Interface ***************************/

type ResponseHeaderBuilder__Updated struct {
	correlationId int32
	version       int
}

func NewResponseHeaderBuilder__Updated() *ResponseHeaderBuilder__Updated {
	return &ResponseHeaderBuilder__Updated{
		version:       1,
		correlationId: -1,
	}
}

func (rb *ResponseHeaderBuilder__Updated) WithCorrelationId(correlationId int32) *ResponseHeaderBuilder__Updated {
	rb.correlationId = correlationId
	return rb
}

func (rb *ResponseHeaderBuilder__Updated) WithVersion(version int) *ResponseHeaderBuilder__Updated {
	if version != 0 && version != 1 {
		panic("CodeCrafters Internal Error: Version has to be >= 0 and <= 1")
	}

	rb.version = version
	return rb
}

func (rb *ResponseHeaderBuilder__Updated) Build() headers.ResponseHeader__Updated {
	if rb.correlationId == -1 {
		panic("CodeCrafters Internal Error: Correlation ID is required")
	}

	return headers.ResponseHeader__Updated{
		Version:       rb.version,
		CorrelationId: rb.correlationId,
	}
}

func BuildResponseHeader__Updated(correlationId int32) headers.ResponseHeader__Updated {
	return NewResponseHeaderBuilder__Updated().WithCorrelationId(correlationId).Build()
}

func buildEmptyResponseHeader__Updated(version int) headers.ResponseHeader__Updated {
	return headers.ResponseHeader__Updated{
		Version: version,
	}
}

func BuildEmptyResponseHeaderv0__Updated() headers.ResponseHeader__Updated {
	return buildEmptyResponseHeader__Updated(0)
}

func BuildEmptyResponseHeaderv1__Updated() headers.ResponseHeader__Updated {
	return buildEmptyResponseHeader__Updated(1)
}
