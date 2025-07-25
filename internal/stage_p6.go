package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testProduce6(stageHarness *test_case_harness.TestCaseHarness) error {
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
	defer client.Close()

	topic := common.TOPIC4_NAME
	partitions := random.RandomInts(0, 3, 2)

	recordBatch1 := builder.NewRecordBatchBuilder().
		AddStringRecord(common.MESSAGE1).
		AddStringRecord(common.MESSAGE2).
		Build()

	recordBatch2 := builder.NewRecordBatchBuilder().
		AddStringRecord(common.MESSAGE3).
		Build()

	request := builder.NewProduceRequestBuilder().
		AddRecordBatch(topic, int32(partitions[0]), recordBatch1).
		AddRecordBatch(topic, int32(partitions[1]), recordBatch2).
		WithCorrelationId(correlationId).
		Build()

	rawResponse, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	actualResponse := builder.NewEmptyProduceResponse()
	if err := actualResponse.Decode(rawResponse.Payload, stageLogger); err != nil {
		return err
	}

	expectedResponse := builder.NewProduceResponseBuilder().
		AddSuccessPartitionResponse(topic, int32(partitions[0])).
		AddSuccessPartitionResponse(topic, int32(partitions[1])).
		WithCorrelationId(correlationId).
		Build()

	if err = assertions.NewProduceResponseAssertion(actualResponse, expectedResponse, stageLogger).Run(stageLogger); err != nil {
		return err
	}

	topicPartitionLogAssertion := assertions.NewTopicPartitionLogAssertion(topic, int32(partitions[0]), []kafkaapi.RecordBatch{recordBatch1}, stageLogger)
	if err = topicPartitionLogAssertion.Run(); err != nil {
		return err
	}

	topicPartitionLogAssertion = assertions.NewTopicPartitionLogAssertion(topic, int32(partitions[1]), []kafkaapi.RecordBatch{recordBatch2}, stageLogger)
	if err = topicPartitionLogAssertion.Run(); err != nil {
		return err
	}

	return nil
}
