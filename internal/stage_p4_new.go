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

func testProduce4new(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger
	err := serializer.GenerateLogDirs(stageLogger, []string{common.TOPIC4_NAME})
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
	partition := int32(random.RandomInt(0, 3))

	recordBatch := builder.NewRecordBatchBuilder().
		AddStringRecord(common.MESSAGE1).
		Build()

	request := builder.NewProduceRequestBuilder().
		AddRecordBatch(topic, partition, recordBatch).
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
		AddSuccessPartitionResponse(topic, partition).
		WithCorrelationId(correlationId).
		Build()

	if err = assertions.NewProduceResponseAssertion(actualResponse, expectedResponse).Run(stageLogger); err != nil {
		return err
	}

	topicPartitionLogAssertion := assertions.NewTopicPartitionLogAssertion(topic, partition, kafkaapi.RecordBatches{recordBatch})
	if err = topicPartitionLogAssertion.Run(stageLogger); err != nil {
		return err
	}

	////////////

	recordBatch2 := builder.NewRecordBatchBuilder().
		AddStringRecord(common.MESSAGE2).
		Build()
	recordBatch2.BaseSequence = 1

	request2 := builder.NewProduceRequestBuilder().
		AddRecordBatch(topic, partition, recordBatch2).
		WithCorrelationId(correlationId).
		Build()

	rawResponse, err = client.SendAndReceive(request2, stageLogger)
	if err != nil {
		return err
	}

	actualResponse = builder.NewEmptyProduceResponse()
	if err := actualResponse.Decode(rawResponse.Payload, stageLogger); err != nil {
		return err
	}

	expectedResponse = builder.NewProduceResponseBuilder().
		AddSuccessPartitionResponse(topic, partition).
		WithCorrelationId(correlationId).
		Build()
	expectedResponse.Body.TopicResponses[0].PartitionResponses[0].BaseOffset = 1

	if err = assertions.NewProduceResponseAssertion(actualResponse, expectedResponse).Run(stageLogger); err != nil {
		return err
	}

	recordBatch2.BaseOffset = 1
	topicPartitionLogAssertion = assertions.NewTopicPartitionLogAssertion(topic, partition, kafkaapi.RecordBatches{recordBatch, recordBatch2})
	if err = topicPartitionLogAssertion.Run(stageLogger); err != nil {
		return err
	}

	return nil
}
