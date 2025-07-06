package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_broker"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionWithFetchKey(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), false)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	broker := kafka_broker.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(broker *kafka_broker.Broker) {
		_ = broker.Close()
	}(broker)

	request := kafkaapi.ApiVersionsRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        18,
			ApiVersion:    4,
			CorrelationId: correlationId,
			ClientId:      "kafka-cli",
		},
		Body: kafkaapi.ApiVersionsRequestBody{
			Version:               4,
			ClientSoftwareName:    "kafka-cli",
			ClientSoftwareVersion: "0.1",
		},
	}

	message := kafkaapi.EncodeApiVersionsRequest(&request)
	stageLogger.Infof("Sending \"ApiVersions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", protocol.GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	stageLogger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", protocol.GetFormattedHexdump(response.RawBytes))

	responseHeader, responseBody, err := kafkaapi.DecodeApiVersionsHeaderAndResponse(response.Payload, 3, stageLogger)
	if err != nil {
		return err
	}

	expectedResponseHeader := kafkaapi.ResponseHeader{
		CorrelationId: correlationId,
	}
	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Evaluate([]string{"CorrelationId"}, stageLogger); err != nil {
		return err
	}

	expectedApiVersionResponse := kafkaapi.ApiVersionsResponse{
		Version:   3,
		ErrorCode: 0,
		ApiKeys: []kafkaapi.ApiVersionsResponseKey{
			{
				ApiKey:     1,
				MaxVersion: 16,
				MinVersion: 0,
			},
			{
				ApiKey:     18,
				MaxVersion: 4,
				MinVersion: 0,
			},
		},
	}

	if err = assertions.NewApiVersionsResponseAssertion(*responseBody, expectedApiVersionResponse).Evaluate([]string{"ErrorCode"}, true, stageLogger); err != nil {
		return err
	}

	return nil
}
