package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
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
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(broker *protocol.Broker) {
		_ = broker.Close()
	}(broker)

	existingTopic := common.TOPIC3_NAME
	unknownPartition := int32(random.RandomInt(3, 10))
	request := kafkaapi.ProduceRequest{
		Header: builder.NewHeaderBuilder().
			BuildProduceRequestHeader(correlationId),
		Body: builder.NewRequestBuilder("produce").
			AddRecordBatchToTopicPartition(existingTopic, unknownPartition, []string{"Hello from Ryan!"}).
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

	if responseHeader.CorrelationId != int32(correlationId) {
		stageLogger.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
		return err
	}
	stageLogger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)
	stageLogger.Successf("✓ Produce request/response cycle completed!")

	stageLogger.Successf("✓ Produce response body: %v", responseBody.Responses)
	return nil
}
