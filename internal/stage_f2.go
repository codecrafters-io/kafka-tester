package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
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
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(broker *protocol.Broker) {
		_ = broker.Close()
	}(broker)

	request := kafkaapi.FetchRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        1,
			ApiVersion:    16,
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.FetchRequestBody{
			MaxWaitMS:         500,
			MinBytes:          1,
			MaxBytes:          52428800,
			IsolationLevel:    0,
			FetchSessionID:    0,
			FetchSessionEpoch: 0,
			Topics:            []kafkaapi.Topic{},
			ForgottenTopics:   []kafkaapi.ForgottenTopic{},
			RackID:            "",
		},
	}

	message := kafkaapi.EncodeFetchRequest(&request)
	stageLogger.Infof("Sending \"Fetch\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"Fetch\" request: \n%v\n", protocol.GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	stageLogger.Debugf("Hexdump of received \"Fetch\" response: \n%v\n", protocol.GetFormattedHexdump(response.RawBytes))

	responseHeader, responseBody, err := kafkaapi.DecodeFetchHeaderAndResponse(response.Payload, 16, stageLogger)
	if err != nil {
		return err
	}

	expectedResponseHeader := kafkaapi.ResponseHeader{
		CorrelationId: correlationId,
	}
	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Evaluate([]string{"CorrelationId"}, stageLogger); err != nil {
		return err
	}

	expectedFetchResponse := kafkaapi.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionID:      0,
		TopicResponses: []kafkaapi.TopicResponse{},
	}
	return assertions.NewFetchResponseAssertion(*responseBody, expectedFetchResponse, stageLogger).
		AssertBody([]string{"ThrottleTimeMs", "ErrorCode"}).
		AssertNoTopics().
		Run()
}
