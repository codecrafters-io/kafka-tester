package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

// ApiVersions returns api version response or error
func ApiVersions(b *protocol.Broker, request *ApiVersionsRequest) (*ApiVersionsResponse, error) {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 1024))

	header := RequestHeader{
		ApiKey:        18,
		ApiVersion:    request.Version,
		CorrelationId: 0, // ToDo: Don't hardcode the value here
		ClientId:      request.ClientSoftwareName,
	}
	header.Encode(&encoder)
	request.Encode(&encoder)
	message := encoder.PackMessage()

	response, err := b.SendAndReceive(message)
	if err != nil {
		return nil, err
	}

	decoder := decoder.RealDecoder{}
	decoder.Init(response)

	responseHeader := ResponseHeader{}
	if err := responseHeader.Decode(&decoder); err != nil {
		return nil, fmt.Errorf("failed to decode header: %w", err)
	}

	apiVersionsResponse := ApiVersionsResponse{Version: request.Version}
	if err := apiVersionsResponse.Decode(&decoder, request.Version); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &apiVersionsResponse, nil
}

func PrintAPIVersions(response *ApiVersionsResponse) {
	fmt.Printf("API versions supported by the broker are:\n")
	fmt.Println("API Key\tMinVersion\tMaxVersion\t")
	apiVersionKeys := response.ApiKeys
	// For each API, the broker will return the minimum and maximum supported version
	for _, key := range apiVersionKeys {
		fmt.Println(key.ApiKey, "\t", key.MinVersion, "\t", key.MaxVersion)
	}
}
