package internal

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetchWithNoTopics(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return wrapErrorAndDebug(err)
	}

	logger := stageHarness.Logger
	err := serializer.GenerateLogDirs(logger)
	if err != nil {
		return wrapErrorAndDebug(err)
	}

	correlationId := getRandomCorrelationId()

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, logger); err != nil {
		return wrapErrorAndDebug(err)
	}
	defer broker.Close()

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
	logger.Infof("Sending \"Fetch\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return wrapErrorAndDebug(err)
	}
	logger.Debugf("Hexdump of sent \"Fetch\" request: \n%v\n", GetFormattedHexdump(message))
	logger.Debugf("Hexdump of received \"Fetch\" response: \n%v\n", GetFormattedHexdump(response))

	responseHeader, responseBody, err := kafkaapi.DecodeFetchHeaderAndResponse(response, 16, logger)
	if err != nil {
		return wrapErrorAndDebug(err)
	}

	if responseHeader.CorrelationId != correlationId {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
	}
	logger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)

	if responseBody.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", responseBody.ErrorCode)
	}
	logger.Successf("✓ Error code: 0 (NO_ERROR)")

	if len(responseBody.TopicResponses) != 0 {
		return fmt.Errorf("Expected responses to be empty, got %v", responseBody.TopicResponses)
	}
	logger.Successf("✓ Topic responses: %v", responseBody.TopicResponses)

	return nil
}

func listDirectoryContents(path string) {
	files, err := os.ReadDir(path)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	for _, file := range files {
		fmt.Println(file.Name())
	}
}

func printFileContents(path string) {
	contents, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	fmt.Println(string(contents))
}

func wrapErrorAndDebug(err error) error {
	fmt.Println(err.Error())
	listDirectoryContents(".")
	printFileContents("./kafka-output.log")
	return err
}
