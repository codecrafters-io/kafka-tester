package internal

import (
	"bytes"
	"fmt"
	"time"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testProduce4Helper(topic string, partition int32, messages []string, baseOffset int64, broker *protocol.Broker, stageLogger *logger.Logger) (kafkaapi.ProduceRequest, error) {
	correlationId := getRandomCorrelationId()

	var body kafkaapi.ProduceRequestBody
	if baseOffset == 0 {
		body = builder.NewProduceRequestBuilder().
			AddRecordBatchToTopicPartition(topic, partition, messages).
			Build()
	} else {
		body = builder.NewProduceRequestBuilder().
			WithBaseSequence(1).
			AddRecordBatchToTopicPartition(topic, partition, messages).
			Build()
	}

	request := kafkaapi.ProduceRequest{
		Header: builder.NewRequestHeaderBuilder().
			BuildProduceRequestHeader(correlationId),
		Body: body,
	}
	// TODO: Can this be changed in the builder?
	request.Body.Topics[0].Partitions[0].Records[0].PartitionLeaderEpoch = 0

	message := kafkaapi.EncodeProduceRequest(&request)
	stageLogger.Infof("Sending \"Produce\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"Produce\" request: \n%v\n", GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message)
	if err != nil {
		stageLogger.Errorf("Failed to send and receive \"Produce\" request: %v", err)
		return kafkaapi.ProduceRequest{}, err
	}
	stageLogger.Debugf("Hexdump of received \"Produce\" response: \n%v\n", GetFormattedHexdump(response.RawBytes))

	responseHeader, responseBody, err := kafkaapi.DecodeProduceHeaderAndResponse(response.Payload, 11, stageLogger)
	if err != nil {
		stageLogger.Errorf("Failed to decode \"Produce\" response header: %v", err)
		return kafkaapi.ProduceRequest{}, err
	}

	var expectedResponse kafkaapi.ProduceResponse
	if baseOffset == 0 {
		expectedResponse = builder.NewProduceResponseBuilder().
			AddTopicPartitionResponse(topic, partition, 0).
			Build(correlationId)
	} else {
		expectedResponse = builder.NewProduceResponseBuilder().
			AddTopicPartitionResponseWithBaseOffset(topic, partition, 0, baseOffset).
			Build(correlationId)
	}

	headerAssertion := assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponse.Header, stageLogger)
	err = headerAssertion.AssertHeader([]string{"CorrelationId"}).Run()
	if err != nil {
		return kafkaapi.ProduceRequest{}, err
	}

	bodyAssertion := assertions.NewProduceResponseAssertion(*responseBody, expectedResponse.Body, stageLogger)
	err = bodyAssertion.AssertBody([]string{"ThrottleTimeMs"}).
		AssertTopics([]string{"Name"}, []string{"ErrorCode", "Index", "BaseOffset", "LogStartOffset", "LogAppendTimeMs"}).
		Run()
	if err != nil {
		return kafkaapi.ProduceRequest{}, err
	}

	return request, nil
}

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

	request1, err := testProduce4Helper(existingTopic, existingPartition, []string{common.HELLO_MSG1}, 0, broker, stageLogger)
	if err != nil {
		return err
	}

	// Validate RecordBatch in log file
	// expectedBatch := buildExpectedRecordBatchForStageP4()
	// err = validateRecordBatchInLogFile(existingTopic, existingPartition, expectedBatch, stageLogger)
	// if err != nil {
	// 	return err
	// }

	stageLogger.Infof("Validated RecordBatch in log file for topic %s, partition %d", existingTopic, existingPartition)
	stageLogger.Infof("Sleeping for 1 second")
	time.Sleep(1 * time.Second)

	// 2

	request2, err := testProduce4Helper(existingTopic, existingPartition, []string{common.HELLO_MSG2}, 1, broker, stageLogger)
	if err != nil {
		return err
	}

	// Validate RecordBatch in log file
	encodedBatches, err := encodeRecordBatchesInLogFile(existingTopic, existingPartition, stageLogger)
	if err != nil {
		return err
	}

	request2.Body.Topics[0].Partitions[0].Records[0].BaseOffset = 1
	as := serializer.GetAnyEncodedBytes(request1.Body.Topics[0].Partitions[0].Records[0])
	as2 := serializer.GetAnyEncodedBytes(request2.Body.Topics[0].Partitions[0].Records[0])

	if bytes.Equal(as, encodedBatches[0]) && bytes.Equal(as2, encodedBatches[1]) {
		stageLogger.Infof("RecordBatches in log file match expected RecordBatches")
	} else {
		fmt.Println("as", as)
		fmt.Println("encodedBatches[0]", encodedBatches[0])
		fmt.Println("as2", as2)
		fmt.Println("encodedBatches[1]", encodedBatches[1])
		stageLogger.Errorf("RecordBatches in log file do not match expected RecordBatches")
		return fmt.Errorf("RecordBatches in log file do not match expected RecordBatches")
	}

	return nil
}
