package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions_legacy"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetchWithNoTopics(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger
	err := serializer.GenerateLogDirs(stageLogger, false)
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
	defer func(client *kafka_client.Client) {
		_ = client.Close()
	}(client)

	request := kafkaapi_legacy.FetchRequest{
		Header: builder_legacy.NewRequestHeaderBuilder().BuildFetchRequestHeader(correlationId),
		Body: kafkaapi_legacy.FetchRequestBody{
			MaxWaitMS:         500,
			MinBytes:          1,
			MaxBytes:          52428800,
			IsolationLevel:    0,
			FetchSessionID:    0,
			FetchSessionEpoch: 0,
			Topics:            []kafkaapi_legacy.Topic{},
			ForgottenTopics:   []kafkaapi_legacy.ForgottenTopic{},
			RackID:            "",
		},
	}

	response, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	responseHeader, responseBody, err := kafkaapi_legacy.DecodeFetchHeaderAndResponse(response.Payload, 16, stageLogger)
	if err != nil {
		return err
	}

	expectedResponseHeader := builder_legacy.BuildResponseHeader(correlationId)
	if err = assertions_legacy.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Run(stageLogger); err != nil {
		return err
	}

	expectedFetchResponse := kafkaapi_legacy.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionID:      0,
		TopicResponses: []kafkaapi_legacy.TopicResponse{},
	}
	return assertions_legacy.NewFetchResponseAssertion(*responseBody, expectedFetchResponse, stageLogger).
		SkipRecordBatches().
		Run(stageLogger)
}
