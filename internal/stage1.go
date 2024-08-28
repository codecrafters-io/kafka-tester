package internal

import (
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

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
