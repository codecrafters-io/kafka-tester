package internal

import (
	"fmt"
	"math"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetchError2(stageHarness *test_case_harness.TestCaseHarness) error {
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

	// Passsing an impossible topic UUID also leads to -1: unexpected server error
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
					TopicUUID: "00000000-0000-0000-0000-000000000000",
					Partitions: []kafkaapi.Partition{
						{
							ID:                 0,
							CurrentLeaderEpoch: 0,
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
		return fmt.Errorf("expected correlationId to be %v, got %v", correlationId, responseHeader.CorrelationId)
	}
	logger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)

	if responseBody.ErrorCode != -1 {
		return fmt.Errorf("expected error code to be -1, got %v", responseBody.ErrorCode)
	}
	logger.Successf("✓ ErrorCode: %v", responseBody.ErrorCode)

	return nil
}
