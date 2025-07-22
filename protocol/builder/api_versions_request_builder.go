package builder

import kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"

type ApiVersionsRequestBuilder struct {
	Version               int16
	ClientSoftwareName    string
	ClientSoftwareVersion string
}

func NewApiVersionsRequestBuilder() *ApiVersionsRequestBuilder {
	return &ApiVersionsRequestBuilder{
		Version:               4,
		ClientSoftwareName:    "kafka-cli",
		ClientSoftwareVersion: "0.1",
	}
}

func (b *ApiVersionsRequestBuilder) Build() kafkaapi.ApiVersionsRequestBody {
	return kafkaapi.ApiVersionsRequestBody{
		Version:               b.Version,
		ClientSoftwareName:    b.ClientSoftwareName,
		ClientSoftwareVersion: b.ClientSoftwareVersion,
	}
}
