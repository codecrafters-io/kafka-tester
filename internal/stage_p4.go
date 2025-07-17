package internal

import (
	"fmt"
	"time"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
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

	// 1

	existingTopic := common.TOPIC4_NAME
	existingPartition := int32(1)
	request1 := kafkaapi.ProduceRequest{
		Header: builder.NewRequestHeaderBuilder().
			BuildProduceRequestHeader(correlationId),
		Body: builder.NewProduceRequestBuilder().
			AddRecordBatchToTopicPartition(existingTopic, existingPartition, []string{common.HELLO_MSG1}).
			Build(),
	}

	message := kafkaapi.EncodeProduceRequest(&request1)
	stageLogger.Infof("Sending \"Produce\" (version: %v) request (Correlation id: %v)", request1.Header.ApiVersion, request1.Header.CorrelationId)
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
		AddTopicPartitionResponse(existingTopic, existingPartition, 0).
		Build(correlationId)

	headerAssertion := assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponse.Header, stageLogger)
	err = headerAssertion.AssertHeader([]string{"CorrelationId"}).Run()
	if err != nil {
		return err
	}

	bodyAssertion := assertions.NewProduceResponseAssertion(*responseBody, expectedResponse.Body, stageLogger)
	err = bodyAssertion.AssertBody([]string{"ThrottleTimeMs"}).
		AssertTopics([]string{"Name"}, []string{"ErrorCode", "Index", "BaseOffset", "LogStartOffset", "LogAppendTimeMs"}).
		Run()
	if err != nil {
		return err
	}

	// Validate RecordBatch in log file
	expectedBatch := buildExpectedRecordBatchForStageP4()
	err = validateRecordBatchInLogFile(existingTopic, existingPartition, expectedBatch, stageLogger)
	if err != nil {
		return err
	}

	stageLogger.Infof("Validated RecordBatch in log file for topic %s, partition %d", existingTopic, existingPartition)
	stageLogger.Infof("Sleeping for 1 second")
	time.Sleep(1 * time.Second)

	// 2

	correlationId2 := getRandomCorrelationId()
	request2 := kafkaapi.ProduceRequest{
		Header: builder.NewRequestHeaderBuilder().
			BuildProduceRequestHeader(correlationId2),
		Body: builder.NewProduceRequestBuilder().
			WithBaseSequence(1).
			AddRecordBatchToTopicPartition(existingTopic, existingPartition, []string{common.HELLO_MSG2}).
			Build(),
	}

	message = kafkaapi.EncodeProduceRequest(&request2)
	stageLogger.Infof("Sending \"Produce\" (version: %v) request (Correlation id: %v)", request2.Header.ApiVersion, request2.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"Produce\" request: \n%v\n", GetFormattedHexdump(message))

	response, err = broker.SendAndReceive(message)
	if err != nil {
		stageLogger.Errorf("Failed to send and receive \"Produce\" request: %v", err)
		return err
	}
	stageLogger.Debugf("Hexdump of received \"Produce\" response: \n%v\n", GetFormattedHexdump(response.RawBytes))

	responseHeader, responseBody, err = kafkaapi.DecodeProduceHeaderAndResponse(response.Payload, 11, stageLogger)
	if err != nil {
		stageLogger.Errorf("Failed to decode \"Produce\" response header: %v", err)
		return err
	}

	expectedResponse = builder.NewProduceResponseBuilder().
		AddTopicPartitionResponseWithBaseOffset(existingTopic, existingPartition, 0, 1).
		Build(correlationId2)

	headerAssertion = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponse.Header, stageLogger)
	err = headerAssertion.AssertHeader([]string{"CorrelationId"}).Run()
	if err != nil {
		return err
	}

	bodyAssertion = assertions.NewProduceResponseAssertion(*responseBody, expectedResponse.Body, stageLogger)
	// "LogAppendTimeMs" is not -1 if it's not latest message
	err = bodyAssertion.AssertBody([]string{"ThrottleTimeMs"}).
		AssertTopics([]string{"Name"}, []string{"ErrorCode", "Index", "BaseOffset", "LogStartOffset"}).
		Run()
	if err != nil {
		return err
	}

	// Validate RecordBatch in log file
	err = validateMultipleRecordBatchesInLogFile(existingTopic, existingPartition, []kafkaapi.RecordBatch{expectedBatch, expectedBatch}, stageLogger)
	if err != nil {
		return err
	}

	fmt.Println(request1.Body.Topics[0].Partitions[0].Records)
	fmt.Println(request2.Body.Topics[0].Partitions[0].Records)

	as := serializer.GetEncodedBytes(request1.Body.Topics[0].Partitions[0].Records)
	as2 := serializer.GetEncodedBytes(request2.Body.Topics[0].Partitions[0].Records)
	fmt.Println(as)
	fmt.Println(as2)

	return nil
}
