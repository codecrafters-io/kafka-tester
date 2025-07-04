package internal

import (
	"time"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testProduce4(stageHarness *test_case_harness.TestCaseHarness) error {
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

	existingTopic := common.TOPIC3_NAME
	existingPartition := int32(1)
	request := kafkaapi.ProduceRequest{
		Header: builder.NewHeaderBuilder().
			BuildProduceRequestHeader(correlationId),
		Body: builder.NewRequestBuilder("produce").
			WithTopic(existingTopic).
			WithPartition(existingPartition).
			WithRecordBatch("Hello from Ryan!").
			BuildProduceRequest(),
	}

	// request := getSuccessRequest(correlationId)

	message := kafkaapi.EncodeProduceRequest(&request)
	stageLogger.Infof("Sending \"Produce\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"Produce\" request: \n%v\n", GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message)
	if err != nil {
		stageLogger.Errorf("Failed to send and receive \"Produce\" request: %v", err)
		return err
	}
	stageLogger.Debugf("Hexdump of received \"Produce\" response: \n%v\n", GetFormattedHexdump(response.RawBytes))

	responseHeader, responseBody, err := kafkaapi.DecodeProduceHeaderAndResponse(response.Payload, 11, stageLogger)
	if err != nil {
		stageLogger.Errorf("Failed to decode \"Produce\" response header: %v", err)
		return err
	}

	if responseHeader.CorrelationId != int32(correlationId) {
		stageLogger.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
		return err
	}
	stageLogger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)
	stageLogger.Successf("✓ Produce request/response cycle completed!")

	// TODO: Add response body assertions
	stageLogger.Successf("✓ Produce response body: %v", responseBody.Responses)
	return nil
}

func getSuccessRequest(correlationId int32) kafkaapi.ProduceRequest {
	// Create a simple record to produce
	record := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Hello Ryan"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	// Create a record batch containing our record
	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       time.Now().UnixMilli(), // Time stamps need to be different else data gets overwritten
		MaxTimestamp:         time.Now().UnixMilli(), // Time stamps need to be different else data gets overwritten
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0, // Just need to update the base sequence for successive requests
		Records:              []kafkaapi.Record{record},
	}

	request := kafkaapi.ProduceRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        0,  // Produce API
			ApiVersion:    11, // Use version 11
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.ProduceRequestBody{
			TransactionalID: "", // Empty string for non-transactional
			Acks:            1,  // Wait for leader acknowledgment
			TimeoutMs:       5000,
			Topics: []kafkaapi.TopicData{
				{
					Name: common.TOPIC3_NAME, // Existing topic with 3 partitions (0, 1, 2)
					Partitions: []kafkaapi.PartitionData{
						{
							Index:   0, // Valid partition
							Records: []kafkaapi.RecordBatch{recordBatch},
						},
					},
				},
			},
		},
	}

	return request
}
