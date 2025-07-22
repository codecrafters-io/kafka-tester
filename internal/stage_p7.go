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

func testProduce7(stageHarness *test_case_harness.TestCaseHarness) error {
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

	topic1 := common.TOPIC2_NAME
	topic2 := common.TOPIC4_NAME
	topic1Partition1 := int32(0)
	topic2Partition1 := int32(random.RandomInt(0, 3))

	recordBatch1 := builder.NewRecordBatchBuilder().
		WithPartitionLeaderEpoch(0).
		AddStringRecord(common.HELLO_MSG1).
		Build()

	recordBatch2 := builder.NewRecordBatchBuilder().
		WithPartitionLeaderEpoch(0).
		AddStringRecord(common.HELLO_MSG2).
		Build()

	request := builder.NewProduceRequestBuilder().
		AddRecordBatch(topic1, topic1Partition1, recordBatch1).
		AddRecordBatch(topic2, topic2Partition1, recordBatch2).
		Build(correlationId)

	response, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	responseHeader, responseBody, err := kafkaapi.DecodeProduceHeaderAndResponse(response.Payload, 11, stageLogger)
	if err != nil {
		stageLogger.Errorf("Failed to decode \"Produce\" response header: %v", err)
		return err
	}

	expectedResponse := builder.NewProduceResponseBuilder().
		AddTopicPartitionResponse(topic1, topic1Partition1, 0).
		AddTopicPartitionResponse(topic2, topic2Partition1, 0).
		Build(correlationId)

	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponse.Header).Evaluate([]string{"CorrelationId"}, stageLogger); err != nil {
		return err
	}

	if err = assertions.NewProduceResponseAssertion(*responseBody, expectedResponse.Body, stageLogger).Run(stageLogger); err != nil {
		return err
	}

	topicPartitionLogAssertion := assertions.NewTopicPartitionLogAssertion(topic1, topic1Partition1, []kafkaapi.RecordBatch{request.Body.Topics[0].Partitions[0].RecordBatches[0]}, stageLogger)
	err = topicPartitionLogAssertion.Run()
	if err != nil {
		return err
	}

	topicPartitionLogAssertion = assertions.NewTopicPartitionLogAssertion(topic2, topic2Partition1, []kafkaapi.RecordBatch{request.Body.Topics[1].Partitions[0].RecordBatches[0]}, stageLogger)
	err = topicPartitionLogAssertion.Run()
	if err != nil {
		return err
	}

	return nil
}
