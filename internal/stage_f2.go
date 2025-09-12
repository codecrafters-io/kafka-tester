package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/response_asserter"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/kafka-tester/internal/response_decoders"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_generator"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetchWithNoTopics(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	if err := kafka_files_generator.NewFilesHandler(logger.GetQuietLogger("")).GenerateServerConfiguration(); err != nil {
		return err
	}

	b := kafka_executable.NewKafkaExecutable(stageHarness)

	if err := b.Run(); err != nil {
		return err
	}

	client := kafka_client.NewClient("localhost:9092")

	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}

	defer client.Close()

	correlationId := getRandomCorrelationId()
	sessionId := 0

	request := builder.NewFetchRequstWithEmptyTopicsBuilder().
		WithCorrelationId(correlationId).
		WithSessionId(int32(sessionId)).
		Build()

	rawResponse, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	assertion := response_assertions.NewFetchResponseAssertion().
		ExpectCorrelationId(correlationId).
		ExpectSessionId(int32(sessionId)).
		ExpectErrorCodeInBody(0).
		ExpectThrottleTimeMs(0)

	_, err = response_asserter.ResponseAsserter[kafkaapi.FetchResponse]{
		DecodeFunc: response_decoders.DecodeFetchResponse,
		Assertion:  assertion,
		Logger:     stageLogger,
	}.DecodeAndAssert(rawResponse.Payload)

	return err

	// request := legacy_kafkaapi.FetchRequest{
	// 	Header: legacy_builder.NewRequestHeaderBuilder().BuildFetchRequestHeader(correlationId),
	// 	Body: legacy_kafkaapi.FetchRequestBody{
	// 		MaxWaitMS:         500,
	// 		MinBytes:          1,
	// 		MaxBytes:          52428800,
	// 		IsolationLevel:    0,
	// 		FetchSessionID:    0,
	// 		FetchSessionEpoch: 0,
	// 		Topics:            []legacy_kafkaapi.Topic{},
	// 		ForgottenTopics:   []legacy_kafkaapi.ForgottenTopic{},
	// 		RackID:            "",
	// 	},
	// }

	// responseHeader, responseBody, err := legacy_kafkaapi.DecodeFetchHeaderAndResponse(response.Payload, 16, stageLogger)
	// if err != nil {
	// 	return err
	// }

	// expectedResponseHeader := legacy_builder.BuildResponseHeader(correlationId)
	// if err = legacy_assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Run(stageLogger); err != nil {
	// 	return err
	// }

	// expectedFetchResponse := legacy_kafkaapi.FetchResponse{
	// 	ThrottleTimeMs: 0,
	// 	ErrorCode:      0,
	// 	SessionID:      0,
	// 	TopicResponses: []legacy_kafkaapi.TopicResponse{},
	// }
	// return legacy_assertions.NewFetchResponseAssertion(*responseBody, expectedFetchResponse, stageLogger).
	// 	SkipRecordBatches().
	// 	Run(stageLogger)
}
