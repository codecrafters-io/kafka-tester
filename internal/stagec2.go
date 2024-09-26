package internal

import (
	"math"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testConcurrentRequests(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger
	err := serializer.GenerateLogDirs(logger)
	if err != nil {
		return err
	}

	clientCount := random.RandomInt(2, 4)
	clients := make([]*protocol.Broker, clientCount)
	correlationIds := make([]int32, clientCount)

	for i := 0; i < clientCount; i++ {
		clients[i] = protocol.NewBroker("localhost:9092")
		if err := clients[i].ConnectWithRetries(b, logger); err != nil {
			return err
		}
	}

	for i, client := range clients {
		correlationIds[i] = int32(random.RandomInt(-math.MaxInt32, math.MaxInt32))
		request := kafkaapi.ApiVersionsRequest{
			Header: kafkaapi.RequestHeader{
				ApiKey:        18,
				ApiVersion:    4,
				CorrelationId: correlationIds[i],
				ClientId:      "kafka-cli",
			},
			Body: kafkaapi.ApiVersionsRequestBody{
				Version:               4,
				ClientSoftwareName:    "kafka-cli",
				ClientSoftwareVersion: "0.1",
			},
		}

		message := kafkaapi.EncodeApiVersionsRequest(&request)
		logger.Infof("Sending request %v of %v: \"ApiVersions\" (version: %v) request (Correlation id: %v)", i+1, clientCount, request.Header.ApiVersion, request.Header.CorrelationId)

		err := client.Send(message)
		if err != nil {
			return err
		}
		logger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", GetFormattedHexdump(message))
	}

	for idx := range clients {
		j := len(clients) - idx - 1
		client := clients[j]

		response, err := client.Receive()
		if err != nil {
			return err
		}
		logger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", GetFormattedHexdump(response))

		responseHeader, responseBody, err := kafkaapi.DecodeApiVersionsHeaderAndResponse(response, 3, logger)
		if err != nil {
			return err
		}

		if err = assertions.NewResponseHeaderAssertion(*responseHeader, kafkaapi.ResponseHeader{
			CorrelationId: correlationIds[j],
		}).Evaluate([]string{"CorrelationId"}, logger); err != nil {
			return err
		}

		expectedApiVersionResponse := kafkaapi.ApiVersionsResponse{
			Version:   4,
			ErrorCode: 0,
			ApiKeys: []kafkaapi.ApiVersionsResponseKey{
				{
					ApiKey:     18,
					MaxVersion: 4,
					MinVersion: 0,
				},
			},
		}
		if err = assertions.NewApiVersionsResponseAssertion(*responseBody, expectedApiVersionResponse).Evaluate([]string{"ErrorCode"}, true, logger); err != nil {
			return err
		}

		logger.Successf("✓ Test %v of %v: Passed", j+1, clientCount)
	}

	for _, client := range clients {
		client.Close()
	}

	return nil
}
