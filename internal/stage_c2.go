package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/response_asserter"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/kafka-tester/internal/response_decoders"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_handler"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testConcurrentRequests(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger

	if err := kafka_files_handler.NewFilesHandler(logger.GetQuietLogger("")).GenerateServerConfiguration(); err != nil {
		return err
	}

	if err := b.Run(); err != nil {
		return err
	}

	clientCount := random.RandomInt(2, 4)
	clients := kafka_client.SpawnMultipleClients(clientCount, "localhost:9092", stageLogger)
	correlationIds := make([]int32, clientCount)
	apiName := utils.APIKeyToName(18)

	// Connect from all clients
	for i := range clientCount {
		if err := clients[i].ConnectWithRetries(b, stageLogger); err != nil {
			return err
		}
	}

	for _, client := range clients {
		defer client.Close()
	}

	for i, client := range clients {
		correlationIds[i] = getRandomCorrelationId()
		request := builder.NewApiVersionsRequestBuilder().WithCorrelationId(correlationIds[i]).Build()
		err := client.Send(request, stageLogger)

		if err != nil {
			return err
		}
	}

	for testIndex := range clients {
		clientIdx := len(clients) - testIndex - 1
		client := clients[clientIdx]
		correlationId := correlationIds[clientIdx]
		rawResponse, err := client.Receive(apiName, stageLogger)

		if err != nil {
			return err
		}

		assertion := response_assertions.NewApiVersionsResponseAssertion().
			ExpectCorrelationId(correlationId).
			ExpectErrorCode(0).
			ExpectApiKeyEntry(18, 0, 4)

		_, err = response_asserter.ResponseAsserter[kafkaapi.ApiVersionsResponse]{
			DecodeFunc: response_decoders.DecodeApiVersionsResponse,
			Assertion:  assertion,
			Logger:     stageLogger,
		}.DecodeAndAssert(rawResponse.Payload)

		if err != nil {
			return err
		}

		stageLogger.Successf("✓ Test %v of %v: Passed", testIndex+1, clientCount)
	}

	return nil
}
