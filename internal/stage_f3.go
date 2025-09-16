package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/legacy_assertions"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_builder"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetchWithUnknownTopicId(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger
	err := legacy_serializer.GenerateLogDirs(stageLogger, false)
	if err != nil {
		return err
	}

	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()
	UUID := common.TOPICX_UUID
	// ToDo: Research on what is NULL v Empty arrays

	client := legacy_kafka_client.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *legacy_kafka_client.Client) {
		_ = client.Close()
	}(client)

	request := legacy_kafkaapi.FetchRequest{
		Header: legacy_builder.NewRequestHeaderBuilder().BuildFetchRequestHeader(correlationId),
		Body: legacy_kafkaapi.FetchRequestBody{
			MaxWaitMS:         500,
			MinBytes:          1,
			MaxBytes:          52428800,
			IsolationLevel:    0,
			FetchSessionID:    0,
			FetchSessionEpoch: 0,
			Topics: []legacy_kafkaapi.Topic{
				{
					TopicUUID: UUID,
					Partitions: []legacy_kafkaapi.Partition{
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
			ForgottenTopics: []legacy_kafkaapi.ForgottenTopic{},
			RackID:          "",
		},
	}

	response, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	responseHeader, responseBody, err := legacy_kafkaapi.DecodeFetchHeaderAndResponse(response.Payload, 16, stageLogger)
	if err != nil {
		return err
	}

	expectedResponseHeader := legacy_builder.BuildResponseHeader(correlationId)
	if err = legacy_assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Run(stageLogger); err != nil {
		return err
	}

	expectedFetchResponse := legacy_kafkaapi.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionID:      0,
		TopicResponses: []legacy_kafkaapi.TopicResponse{
			{
				Topic: UUID,
				PartitionResponses: []legacy_kafkaapi.PartitionResponse{
					{
						PartitionIndex: 0,
						ErrorCode:      100,
					},
				},
			},
		},
	}

	return legacy_assertions.NewFetchResponseAssertion(*responseBody, expectedFetchResponse, stageLogger).
		SkipRecordBatches().
		Run(stageLogger)
}
