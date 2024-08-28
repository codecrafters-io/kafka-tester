package internal

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

func getAPIVersionsV3(broker net.Conn) (*kafkaapi.ApiVersionsResponse, error) {
	request := kafkaapi.ApiVersionsRequest{Version: 3, ClientSoftwareName: "kafka-cli", ClientSoftwareVersion: "0.1"}

	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 1024))

	header := kafkaapi.RequestHeader{
		ApiKey:        18,
		ApiVersion:    request.Version,
		CorrelationId: 0, // ToDo: Don't hardcode the value here
		ClientId:      request.ClientSoftwareName,
	}
	header.Encode(&encoder)
	request.Encode(&encoder)
	message := protocol.PackMessage(&encoder)

	broker.Write(message)

	response := make([]byte, 4) // length
	broker.Read(response)
	length := int32(binary.BigEndian.Uint32(response))

	response = make([]byte, length)
	broker.Read(response)
	protocol.PrintHexdump(response)

	decoder := decoder.RealDecoder{}
	decoder.Init(response)

	responseHeader := kafkaapi.ResponseHeader{}
	if err := responseHeader.Decode(&decoder); err != nil {
		return nil, fmt.Errorf("failed to decode header: %w", err)
	}

	apiVersionsResponse := kafkaapi.ApiVersionsResponse{Version: request.Version}
	if err := apiVersionsResponse.Decode(&decoder, request.Version); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &apiVersionsResponse, nil
}

func GetAPIVersions(prettyPrint bool) {
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.Connect(); err != nil {
		panic(err)
	}
	defer broker.Close()

	response, err := kafkaapi.ApiVersions(broker, &kafkaapi.ApiVersionsRequest{Version: 3, ClientSoftwareName: "kafka-cli", ClientSoftwareVersion: "0.1"})
	if err != nil {
		panic(err)
	}

	if prettyPrint {
		kafkaapi.PrintAPIVersions(response)
	}
}
