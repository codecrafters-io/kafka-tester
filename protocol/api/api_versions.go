package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

func GetAPIVersions(prettyPrint bool) {
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.Connect(); err != nil {
		panic(err)
	}
	defer broker.Close()

	response, err := ApiVersions(broker, &ApiVersionsRequest{Version: 3, ClientSoftwareName: "kafka-cli", ClientSoftwareVersion: "0.1"})
	if err != nil {
		panic(err)
	}

	if prettyPrint {
		PrintAPIVersions(response)
	}
}

func EncodeApiVersionsRequest(header *RequestHeader, request *ApiVersionsRequest) ([]byte, error) {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 1024))

	header.Encode(&encoder)
	request.Encode(&encoder)
	message := encoder.PackMessage()

	return message, nil
}

func DecodeApiVersionsResponse(response []byte, version int16) (*ResponseHeader, *ApiVersionsResponse, error) {
	decoder := decoder.RealDecoder{}
	decoder.Init(response)

	responseHeader := ResponseHeader{}
	if err := responseHeader.Decode(&decoder); err != nil {
		return nil, nil, fmt.Errorf("failed to decode header: %w", err)
	}

	apiVersionsResponse := ApiVersionsResponse{Version: version}
	if err := apiVersionsResponse.Decode(&decoder, version); err != nil {
		return nil, nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &responseHeader, &apiVersionsResponse, nil
}

// ApiVersions returns api version response or error
func ApiVersions(b *protocol.Broker, request *ApiVersionsRequest) (*ApiVersionsResponse, error) {
	header := RequestHeader{
		ApiKey:        18,
		ApiVersion:    request.Version,
		CorrelationId: 0, // ToDo: Don't hardcode the value here
		ClientId:      request.ClientSoftwareName,
	}
	message, err := EncodeApiVersionsRequest(&header, request)
	if err != nil {
		return nil, err
	}

	response, err := b.SendAndReceive(message)
	if err != nil {
		return nil, err
	}

	_, apiVersionsResponse, err := DecodeApiVersionsResponse(response, request.Version)
	if err != nil {
		return nil, err
	}

	return apiVersionsResponse, nil
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
