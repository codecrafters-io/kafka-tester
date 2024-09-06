package internal

import (
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetch(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger

	correlationId := int32(random.RandomInt(-math.MaxInt32, math.MaxInt32))

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
					TopicUUID: "7d98b8a8-4a42-4ec8-a4fa-bce4c95d18a6",
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

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}

	responseHeader, responseBody, err := kafkaapi.DecodeFetchHeaderAndResponse(response, 16)
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

	msgValues := []string{}
	expectedMsgValues := []string{"m1", "m2", "m3"}
	for _, topic := range responseBody.Responses {
		for _, partition := range topic.Partitions {
			if len(partition.Records) == 0 {
				return fmt.Errorf("Expected partition.Records to have length greater than 0, got %v", len(partition.Records))
			}
			for _, record := range partition.Records {
				if len(record.Records) == 0 {
					return fmt.Errorf("Expected record.Records to have length greater than 0, got %v", len(record.Records))
				}
				for _, r := range record.Records {
					if r.Value == nil {
						return fmt.Errorf("Expected record.Value to not be nil")
					}
					msgValues = append(msgValues, string(r.Value))
				}
			}
		}
	}

	sort.Strings(msgValues)
	sort.Strings(expectedMsgValues)

	if !reflect.DeepEqual(msgValues, expectedMsgValues) {
		return fmt.Errorf("Expected message values to be %v, got %v", expectedMsgValues, msgValues)
	}

	logger.Successf("✓ Messages: %q", msgValues)

	return nil
}
