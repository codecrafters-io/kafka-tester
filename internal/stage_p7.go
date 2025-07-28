package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testProduce7(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger
	err := serializer.GenerateLogDirs(stageLogger, []string{common.TOPIC2_NAME, common.TOPIC4_NAME})
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

	topic1 := common.TOPIC2_NAME
	topic2 := common.TOPIC4_NAME
	topic1Partition1 := int32(0)
	topic2Partition1 := int32(0)
	topic2Partition2 := int32(1)

	recordBatch1 := builder.NewRecordBatchBuilder().
		AddStringRecord(common.MESSAGE1).
		Build()

	recordBatch2 := builder.NewRecordBatchBuilder().
		AddStringRecord(common.MESSAGE2).
		Build()

	recordBatch3 := builder.NewRecordBatchBuilder().
		AddStringRecord(common.MESSAGE3).
		Build()

	request := builder.NewProduceRequestBuilder().
		AddRecordBatch(topic1, topic1Partition1, recordBatch1).
		AddRecordBatch(topic2, topic2Partition1, recordBatch2).
		AddRecordBatch(topic2, topic2Partition2, recordBatch3).
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
		AddSuccessPartitionResponse(topic1, topic1Partition1).
		AddSuccessPartitionResponse(topic2, topic2Partition1).
		AddSuccessPartitionResponse(topic2, topic2Partition2).
		WithCorrelationId(correlationId).
		Build()

	if err = assertions.NewProduceResponseAssertion(actualResponse, expectedResponse, stageLogger).Run(stageLogger); err != nil {
		return err
	}

	if err = assertions.NewTopicPartitionLogAssertion(topic1, topic1Partition1, kafkaapi.RecordBatches{recordBatch1}).Run(stageLogger); err != nil {
		return err
	}

	if err = assertions.NewTopicPartitionLogAssertion(topic2, topic2Partition1, kafkaapi.RecordBatches{recordBatch2}).Run(stageLogger); err != nil {
		return err
	}

	if err = assertions.NewTopicPartitionLogAssertion(topic2, topic2Partition2, kafkaapi.RecordBatches{recordBatch3}).Run(stageLogger); err != nil {
		return err
	}

	return nil
}
