package internal

import (
	"reflect"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
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
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(broker *protocol.Broker) {
		_ = broker.Close()
	}(broker)

	existingTopic := common.TOPIC4_NAME
	partitions := random.RandomInts(0, 3, 2)
	partition1 := int32(partitions[0])
	partition2 := int32(partitions[1])
	request := kafkaapi.ProduceRequest{
		Header: builder.NewHeaderBuilder().
			BuildProduceRequestHeader(correlationId),
		Body: builder.NewRequestBuilder("produce").
			AddRecordBatchToTopicPartition(existingTopic, partition1, []string{common.HELLO_MSG1}).
			AddRecordBatchToTopicPartition(existingTopic, partition2, []string{common.HELLO_MSG2}).
			BuildProduceRequest(),
	}

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

	expectedResponse := builder.NewProduceResponseBuilder().
		AddTopicPartitionResponse(existingTopic, partition1, 0).
		AddTopicPartitionResponse(existingTopic, partition2, 0).
		Build(correlationId)

	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponse.Header, stageLogger).AssertHeader([]string{"CorrelationId"}).Run(); err != nil {
		return err
	}

	///////////////////////////////////////////////////////////////
	actualResponse := kafkaapi.ProduceResponse{
		Header: *responseHeader,
		Body:   *responseBody,
	}
	if !reflect.DeepEqual(actualResponse, expectedResponse) {
		stageLogger.Errorf("Expected response body to be %v, got %v", expectedResponse, actualResponse)
	}
	stageLogger.Successf("✓ Produce response body: %v", responseBody.Responses)
	///////////////////////////////////////////////////////////////

	assertion := assertions.NewProduceResponseAssertion(*responseBody, expectedResponse.Body, stageLogger)
	err = assertion.AssertBody([]string{"ThrottleTimeMs"}).
		AssertTopics([]string{"Name"}, []string{"ErrorCode", "Index", "BaseOffset", "LogStartOffset", "LogAppendTimeMs"}).
		Run()
	if err != nil {
		return err
	}

	return nil
}
