package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testProduce3(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), false)
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
	defer client.Close()

	existingTopic := common.TOPIC4_NAME
	unknownPartition := int32(random.RandomInt(4, 10))

	recordBatch := builder.NewRecordBatchBuilder().
		AddStringRecord(common.MESSAGE1).
		Build()

	request := builder.NewProduceRequestBuilder().
		AddRecordBatch(existingTopic, unknownPartition, recordBatch).
		WithCorrelationId(correlationId).
		Build()

	rawResponse, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	actualResponse := builder.NewProduceResponseBuilder().BuildEmpty()
	if err := actualResponse.Decode(rawResponse.Payload, stageLogger); err != nil {
		return err
	}

	expectedResponse := builder.NewProduceResponseBuilder().
		AddErrorPartitionResponse(existingTopic, unknownPartition, 3).
		WithCorrelationId(correlationId).
		Build()

	if err = assertions.NewProduceResponseAssertion(actualResponse, expectedResponse, stageLogger).Run(stageLogger); err != nil {
		return err
	}

	return nil
}
