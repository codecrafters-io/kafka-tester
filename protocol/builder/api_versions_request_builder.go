package builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ApiVersionsRequestBuilder struct {
	version               int16
	correlationId         int32
	clientSoftwareName    string
	clientSoftwareVersion string
}

func NewApiVersionsRequestBuilder() *ApiVersionsRequestBuilder {
	return &ApiVersionsRequestBuilder{
		version:               4,
		correlationId:         -1,
		clientSoftwareName:    "kafka-cli",
		clientSoftwareVersion: "0.1",
	}
}

func (b *ApiVersionsRequestBuilder) WithCorrelationId(correlationId int32) *ApiVersionsRequestBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ApiVersionsRequestBuilder) WithVersion(version int16) *ApiVersionsRequestBuilder {
	b.version = version
	return b
}

func (b *ApiVersionsRequestBuilder) Build() kafkaapi.ApiVersionsRequest {
	return kafkaapi.ApiVersionsRequest{
		Header: NewRequestHeaderBuilder().BuildApiVersionsRequestHeader(b.correlationId, b.version),
		Body: kafkaapi.ApiVersionsRequestBody{
			Version:               value.Int16{Value: b.version},
			ClientSoftwareName:    value.CompactString{Value: b.clientSoftwareName},
			ClientSoftwareVersion: value.CompactString{Value: b.clientSoftwareVersion},
		},
	}
}
