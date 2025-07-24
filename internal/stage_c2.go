package internal

import (
	"math"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testConcurrentRequests(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	clientCount := random.RandomInt(2, 4)
	clients := make([]*kafka_client.Client, clientCount)
	correlationIds := make([]int32, clientCount)

	for i := 0; i < clientCount; i++ {
		clients[i] = kafka_client.NewClient("localhost:9092")
		if err := clients[i].ConnectWithRetries(b, stageLogger); err != nil {
			return err
		}
	}

	defer func() {
		for _, client := range clients {
			if client != nil {
				_ = client.Close()
			}
		}
	}()

	for i, client := range clients {
		correlationIds[i] = int32(random.RandomInt(-math.MaxInt32, math.MaxInt32))
		request := builder.NewApiVersionsRequestBuilder().
			WithCorrelationId(correlationIds[i]).
			Build()

		message := request.Encode()
		stageLogger.Infof("Sending request %v of %v: \"ApiVersions\" (version: %v) request (Correlation id: %v)", i+1, clientCount, request.Header.ApiVersion, request.Header.CorrelationId)
		stageLogger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", utils.GetFormattedHexdump(message))

		err := client.Send(message)
		if err != nil {
			return err
		}
	}

	for idx := range clients {
		j := len(clients) - idx - 1
		client := clients[j]
		correlationId := correlationIds[j]

		rawResponse, err := client.Receive()
		if err != nil {
			return err
		}
		stageLogger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", utils.GetFormattedHexdump(rawResponse.RawBytes))

		actualResponse := builder.NewApiVersionsResponseBuilder().BuildEmpty()
		if err := actualResponse.Decode(rawResponse.Payload, stageLogger); err != nil {
			return err
		}

		expectedApiVersionResponse := builder.NewApiVersionsResponseBuilder().
			AddApiKeyEntry(18, 0, 4).
			WithCorrelationId(correlationId).
			Build()

		if err = assertions.NewApiVersionsResponseAssertion(actualResponse, expectedApiVersionResponse).Run(stageLogger); err != nil {
			return err
		}

		stageLogger.Successf("✓ Test %v of %v: Passed", j+1, clientCount)
	}

	return nil
}
