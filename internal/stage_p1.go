package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testProduce1(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), false)
	if err != nil {
		return err
	}

	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	client := kafka_client.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer client.Close()

	request := builder.NewApiVersionsRequestBuilder().
		WithCorrelationId(correlationId).
		Build()

	response, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	responseHeader, responseBody, err := kafkaapi.DecodeApiVersionsHeaderAndResponse(response.Payload, 3, stageLogger)
	if err != nil {
		return err
	}

	expectedResponse := builder.NewApiVersionsResponseBuilder().
		AddApiKeyEntry(0, 0, 11).
		AddApiKeyEntry(18, 0, 4).
		Build(correlationId)

	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponse.Header).Run(stageLogger); err != nil {
		return err
	}

	if err = assertions.NewApiVersionsResponseAssertion(*responseBody, expectedResponse.Body).Run(stageLogger); err != nil {
		return err
	}

	return nil
}
