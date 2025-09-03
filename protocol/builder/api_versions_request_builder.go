package builder

import "github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"

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

func (b *ApiVersionsRequestBuilder) Build() kafkaapi.ApiVersionsRequest {
	return kafkaapi.ApiVersionsRequest{
		Header: NewRequestHeaderBuilder().BuildApiVersionsRequestHeader(b.correlationId),
		Body: kafkaapi.ApiVersionsRequestBody{
			Version:               b.version,
			ClientSoftwareName:    b.clientSoftwareName,
			ClientSoftwareVersion: b.clientSoftwareVersion,
		},
	}
}
