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

func testFetchError(stageHarness *test_case_harness.TestCaseHarness) error {
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

	// Error 100 inside partition response
	// TODO: This is not easy to encode for users
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
					TopicUUID: "0f62a58e-617b-462f-9161-132a1946d66a",
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

	// Passsing an impossible topic UUID also leads to no responses
	// request := kafkaapi.FetchRequest{
	// 	Header: kafkaapi.RequestHeader{
	// 		ApiKey:        1,
	// 		ApiVersion:    16,
	// 		CorrelationId: correlationId,
	// 		ClientId:      "kafka-cli",
	// 	},
	// 	Body: kafkaapi.FetchRequestBody{
	// 		MaxWaitMS:         500,
	// 		MinBytes:          1,
	// 		MaxBytes:          52428800,
	// 		IsolationLevel:    0,
	// 		FetchSessionID:    0,
	// 		FetchSessionEpoch: 0,
	// 		Topics: []kafkaapi.Topic{
	// 			{
	// 				TopicUUID: "00000000-0000-0000-0000-000000000000",
	// 				Partitions: []kafkaapi.Partition{
	// 					{
	// 						ID:                 0,
	// 						CurrentLeaderEpoch: 0,
	// 						FetchOffset:        0,
	// 						LastFetchedOffset:  0,
	// 						LogStartOffset:     0,
	// 						PartitionMaxBytes:  1048576,
	// 					},
	// 				},
	// 			},
	// 		},
	// 		ForgottenTopics: []kafkaapi.ForgottenTopic{},
	// 		RackID:          "",
	// 	},
	// }

	// request := kafkaapi.FetchRequest{
	// 	Header: kafkaapi.RequestHeader{
	// 		ApiKey:        1,
	// 		ApiVersion:    16,
	// 		CorrelationId: correlationId,
	// 		ClientId:      "kafka-tester",
	// 	},
	// 	Body: kafkaapi.FetchRequestBody{
	// 		MaxWaitMS:         500,
	// 		MinBytes:          1,
	// 		MaxBytes:          52428800,
	// 		IsolationLevel:    0,
	// 		FetchSessionID:    0,
	// 		FetchSessionEpoch: 0,
	// 		Topics:            []kafkaapi.Topic{},
	// 		ForgottenTopics:   []kafkaapi.ForgottenTopic{},
	// 		RackID:            "",
	// 	},
	// }
	// // No topic give, response also has no topic data, empty responses

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
	logger.Successf("✓ correlationId: %v", responseHeader.CorrelationId)

	// ToDo: Confirm with @paul
	logger.Successf("✓ responseBody.ErrorCode: %v", responseBody.ErrorCode)
	if len(responseBody.Responses) == 0 {
		return fmt.Errorf("expected responses to be non-empty")
	}
	if len(responseBody.Responses[0].Partitions) == 0 {
		return fmt.Errorf("expected partitions to be non-empty")
	}

	errorCode := responseBody.Responses[0].Partitions[0].ErrorCode
	if errorCode != 100 {
		return fmt.Errorf("expected error code to be 100, got %v", errorCode)
	}
	logger.Successf("✓ errorCode: %v", errorCode)

	return nil
}
