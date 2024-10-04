package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	realencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/bytes_diff_visualizer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetchMultipleMessages(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger
	err := serializer.GenerateLogDirs(logger, false)
	if err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

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
					TopicUUID: common.TOPIC3_UUID,
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
	logger.Debugf("Hexdump of sent \"Fetch\" request: \n%v\n", GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	logger.Debugf("Hexdump of received \"Fetch\" response: \n%v\n", GetFormattedHexdump(response))

	responseHeader, responseBody, err := kafkaapi.DecodeFetchHeaderAndResponse(response, 16, logger)
	if err != nil {
		return err
	}

	expectedResponseHeader := kafkaapi.ResponseHeader{
		CorrelationId: correlationId,
	}
	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Evaluate([]string{"CorrelationId"}, logger); err != nil {
		return err
	}

	expectedFetchResponse := kafkaapi.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionID:      0,
		TopicResponses: []kafkaapi.TopicResponse{
			{
				Topic: common.TOPIC3_UUID,
				PartitionResponses: []kafkaapi.PartitionResponse{
					{
						PartitionIndex:      0,
						ErrorCode:           0,
						HighWatermark:       0,
						LastStableOffset:    0,
						LogStartOffset:      0,
						AbortedTransactions: []kafkaapi.AbortedTransaction{},
						RecordBatches: []kafkaapi.RecordBatch{
							{
								BaseOffset:           0,
								BatchLength:          0,
								PartitionLeaderEpoch: 0,
								Magic:                0,
								Attributes:           0,
								LastOffsetDelta:      0,
								FirstTimestamp:       0,
								MaxTimestamp:         0,
								ProducerId:           0,
								ProducerEpoch:        0,
								BaseSequence:         0,
								Records: []kafkaapi.Record{
									{
										Length:         0,
										Attributes:     0,
										TimestampDelta: 0,
										OffsetDelta:    0,
										Key:            []byte{},
										Value:          []byte(common.MESSAGE2),
										Headers:        []kafkaapi.RecordHeader{},
									},
								},
							},
							{
								BaseOffset:           1,
								BatchLength:          0,
								PartitionLeaderEpoch: 0,
								Magic:                0,
								Attributes:           0,
								LastOffsetDelta:      0,
								FirstTimestamp:       1726045973899,
								MaxTimestamp:         1726045973899,
								ProducerId:           0,
								ProducerEpoch:        0,
								BaseSequence:         0,
								Records: []kafkaapi.Record{
									{
										Length:         0,
										Attributes:     0,
										TimestampDelta: 0,
										OffsetDelta:    0,
										Key:            []byte{},
										Value:          []byte(common.MESSAGE3),
										Headers:        []kafkaapi.RecordHeader{},
									},
								},
							},
						},
						PreferedReadReplica: 0,
					},
				},
			},
		},
	}

	expectedRecordBatchBytes := serializeTopicData([]string{common.MESSAGE2, common.MESSAGE3})

	encoder := realencoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))
	responseBody.TopicResponses[0].PartitionResponses[0].RecordBatches[0].Encode(&encoder)
	responseBody.TopicResponses[0].PartitionResponses[0].RecordBatches[1].Encode(&encoder)
	actualRecordBatchBytes := encoder.Bytes()[:encoder.Offset()]

	// fmt.Println(expectedRecordBatchBytes)
	// fmt.Println(actualRecordBatchBytes)
	// fmt.Println(bytes.Equal(expectedRecordBatchBytes, actualRecordBatchBytes))

	result := bytes_diff_visualizer.VisualizeByteDiff(expectedRecordBatchBytes, actualRecordBatchBytes)
	for i := range len(result) {
		fmt.Println(result[i])
	}
	return assertions.NewFetchResponseAssertion(*responseBody, expectedFetchResponse, logger).
		AssertBody([]string{"ThrottleTimeMs", "ErrorCode"}).
		AssertTopics([]string{"Topic"}, []string{"ErrorCode", "PartitionIndex"}, []string{"BaseOffset"}, []string{"Value"}).
		// AssertRecordBatchBytes(). // TODO: why this not working ?
		Run()
}

func serializeTopicData(messages []string) []byte {
	encoder := realencoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	for i, message := range messages {
		recordBatch := kafkaapi.RecordBatch{
			BaseOffset:           int64(i),
			PartitionLeaderEpoch: 0,
			Attributes:           0,
			LastOffsetDelta:      0,
			FirstTimestamp:       1726045973899,
			MaxTimestamp:         1726045973899,
			ProducerId:           0,
			ProducerEpoch:        0,
			BaseSequence:         0,
			Records: []kafkaapi.Record{
				{
					Attributes:     0,
					TimestampDelta: 0,
					Key:            nil,
					Value:          []byte(message),
					Headers:        []kafkaapi.RecordHeader{},
				},
			},
		}
		recordBatch.Encode(&encoder)
	}

	return encoder.Bytes()[:encoder.Offset()]
}
