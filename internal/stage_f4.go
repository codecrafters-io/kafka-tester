package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetchNoMessages(stageHarness *test_case_harness.TestCaseHarness) error {
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

	request := kafkaapi.FetchRequest{
		Header: builder.NewRequestHeaderBuilder().BuildFetchRequestHeader(correlationId),
		Body: kafkaapi.FetchRequestBody{
			MaxWaitMS:         500,
			MinBytes:          1,
			MaxBytes:          52428800,
			IsolationLevel:    0,
			FetchSessionID:    0,
			FetchSessionEpoch: 0,
			Topics: []kafkaapi.Topic{
				{
					TopicUUID: common.TOPIC2_UUID,
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
	stageLogger.Infof("Sending \"Fetch\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"Fetch\" request: \n%v\n", utils.GetFormattedHexdump(message))

	response, err := client.SendAndReceive(message)
	if err != nil {
		return err
	}
	stageLogger.Debugf("Hexdump of received \"Fetch\" response: \n%v\n", utils.GetFormattedHexdump(response.RawBytes))

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
		TopicResponses: []kafkaapi.TopicResponse{
			{
				Topic: common.TOPIC2_UUID,
				PartitionResponses: []kafkaapi.PartitionResponse{
					{
						PartitionIndex:      0,
						ErrorCode:           0,
						HighWatermark:       0,
						LastStableOffset:    0,
						LogStartOffset:      0,
						AbortedTransactions: []kafkaapi.AbortedTransaction{},
						RecordBatches:       []kafkaapi.RecordBatch{},
						PreferedReadReplica: 0,
					},
				},
			},
		},
	}

	return assertions.NewFetchResponseAssertion(*responseBody, expectedFetchResponse, stageLogger).
		AssertBody([]string{"ThrottleTimeMs", "ErrorCode"}).
		AssertTopics([]string{"Topic"}, []string{"ErrorCode", "PartitionIndex"}, nil, nil).
		Run()
}
