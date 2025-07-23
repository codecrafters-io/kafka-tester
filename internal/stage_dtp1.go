package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionWithDescribeTopicPartitions(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	client := kafka_client.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *kafka_client.Client) {
		_ = client.Close()
	}(client)

	request := kafkaapi.ApiVersionsRequest{
		Header: builder.NewRequestHeaderBuilder().BuildApiVersionsRequestHeader(correlationId),
		Body: kafkaapi.ApiVersionsRequestBody{
			Version:               4,
			ClientSoftwareName:    "kafka-cli",
			ClientSoftwareVersion: "0.1",
		},
	}

	message := kafkaapi.EncodeApiVersionsRequest(&request)
	stageLogger.Infof("Sending \"ApiVersions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", utils.GetFormattedHexdump(message))

	response, err := client.SendAndReceive(message)
	if err != nil {
		return err
	}
	stageLogger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", utils.GetFormattedHexdump(response.RawBytes))

	responseHeader, responseBody, err := kafkaapi.DecodeApiVersionsHeaderAndResponse(response.Payload, 3, stageLogger)
	if err != nil {
		return err
	}

	expectedApiVersionResponse := builder.NewApiVersionsResponseBuilder().
		AddApiKeyEntry(18, 0, 4).
		AddApiKeyEntry(75, 0, 0).
		Build(correlationId)

	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedApiVersionResponse.Header).Evaluate([]string{"CorrelationId"}, stageLogger); err != nil {
		return err
	}

	// TODO: Add ApiVersionsResponseAssertion to all stages
	if err = assertions.NewApiVersionsResponseAssertion(*responseBody, expectedApiVersionResponse.Body).Run(stageLogger); err != nil {
		return err
	}

	return nil
}
