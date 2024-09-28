package internal

import (
	"fmt"
	"reflect"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetch(stageHarness *test_case_harness.TestCaseHarness) error {
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
					TopicUUID: common.TOPIC1_UUID,
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

	msgValues := []string{}
	expectedMsgValues := []string{common.MESSAGE1}
	for _, topicResponse := range responseBody.TopicResponses {
		for _, partitionResponse := range topicResponse.PartitionResponses {
			if len(partitionResponse.RecordBatches) == 0 {
				return fmt.Errorf("Expected partition.RecordBatches to have length greater than 0, got %v", len(partitionResponse.RecordBatches))
			}
			for _, recordBatch := range partitionResponse.RecordBatches {
				if len(recordBatch.Records) == 0 {
					return fmt.Errorf("Expected recordBatch.Records to have length greater than 0, got %v", len(recordBatch.Records))
				}
				for _, r := range recordBatch.Records {
					if r.Value == nil {
						return fmt.Errorf("Expected record.Value to not be nil")
					}
					msgValues = append(msgValues, string(r.Value))
				}
			}
		}
	}

	if !reflect.DeepEqual(msgValues, expectedMsgValues) {
		return fmt.Errorf("Expected message values to be %v, got %v", expectedMsgValues, msgValues)
	}

	logger.Successf("✓ Messages: %q", msgValues)

	return nil
}
