package builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

type ApiVersionsResponseBuilder struct {
	version        int16
	correlationId  int32
	errorCode      int16
	apiKeys        []kafkaapi.ApiKeyEntry
	throttleTimeMs int32
}

func NewApiVersionsResponseBuilder() *ApiVersionsResponseBuilder {
	return &ApiVersionsResponseBuilder{
		version:        4,
		correlationId:  -1,
		errorCode:      0,
		throttleTimeMs: 0,
		apiKeys:        []kafkaapi.ApiKeyEntry{},
	}
}

// TODO[PaulRefactor]: Try to remove this, and change how "Decode()" requires an empty response.
func (b *ApiVersionsResponseBuilder) BuildForDecode() kafkaapi.ApiVersionsResponse {
	return kafkaapi.ApiVersionsResponse{
		Header: BuildEmptyResponseHeaderv0(),
		Body:   kafkaapi.ApiVersionsResponseBody{Version: 4},
	}
}
