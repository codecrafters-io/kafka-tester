package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetchWithUnkownTopicID(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger
	err := serializer.GenerateLogDirs(logger)
	if err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()
	UUID := common.TOPICX_UUID
	// ToDo: Research on what is NULL v Empty arrays

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, logger); err != nil {
		return err
	}
	defer broker.Close()

	request := kafkaapi.FetchRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        1,
			ApiVersion:    16,
			CorrelationId: correlationId,
			ClientId:      "kafka-cli",
		},
		Body: kafkaapi.FetchRequestBody{
			MaxWaitMS:         500,
			MinBytes:          1,
			MaxBytes:          52428800,
			IsolationLevel:    0,
			FetchSessionID:    0,
			FetchSessionEpoch: 0,
			Topics: []kafkaapi.Topic{
				{
					TopicUUID: UUID,
					Partitions: []kafkaapi.Partition{
						{
							ID:                 0,
							CurrentLeaderEpoch: -1,
							FetchOffset:        0,
							LastFetchedOffset:  -1,
							LogStartOffset:     -1,
							PartitionMaxBytes:  1048576,
						},
					},
				},
			},
			ForgottenTopics: []kafkaapi.ForgottenTopic{},
			RackID:          "",
		},
	}

	message := kafkaapi.EncodeFetchRequest(&request)
	logger.Infof("Sending \"Fetch\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	logger.Debugf("Hexdump of sent \"Fetch\" request: \n%v\n", GetFormattedHexdump(message))
	logger.Debugf("Hexdump of received \"Fetch\" response: \n%v\n", GetFormattedHexdump(response))

	responseHeader, responseBody, err := kafkaapi.DecodeFetchHeaderAndResponse(response, 16, logger)
	if err != nil {
		return err
	}

	if responseHeader.CorrelationId != correlationId {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
	}
	logger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)

	if responseBody.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", responseBody.ErrorCode)
	}
	logger.Successf("✓ Error code: 0 (NO_ERROR)")

	if len(responseBody.TopicResponses) != 1 {
		return fmt.Errorf("Expected TopicResponses to have length 1, got %v", len(responseBody.TopicResponses))
	}

	topicResponse := responseBody.TopicResponses[0]
	if len(topicResponse.PartitionResponses) != 1 {
		return fmt.Errorf("Expected PartitionResponses to have length 1, got %v", len(topicResponse.PartitionResponses))
	}

	if topicResponse.Topic != UUID {
		return fmt.Errorf("Expected Topic UUID to match UUID from request, got %v", topicResponse.Topic)
	}
	logger.Successf("✓ Topic UUID: %v", topicResponse.Topic)

	partitionResponse := topicResponse.PartitionResponses[0]

	if partitionResponse.ErrorCode != 100 {
		return fmt.Errorf("Expected Error code to be 100, got %v", partitionResponse.ErrorCode)
	}
	logger.Successf("✓ PartitionResponse Error code: 100 (UNKNOWN_TOPIC_ID)")

	if len(partitionResponse.RecordBatches) != 0 {
		return fmt.Errorf("Expected RecordBatches to have length 0, got %v", len(partitionResponse.RecordBatches))
	}
	logger.Successf("✓ RecordBatches: %v", partitionResponse.RecordBatches)

	return nil
}
