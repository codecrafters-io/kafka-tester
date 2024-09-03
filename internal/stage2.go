package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testHardcodedCorrelationId(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, logger); err != nil {
		return err
	}
	defer broker.Close()

	correlationId := 7

	request := kafkaapi.ApiVersionsRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        18,
			ApiVersion:    3,
			CorrelationId: int32(correlationId),
			ClientId:      "kafka-cli",
		},
		Body: kafkaapi.ApiVersionsRequestBody{
			Version:               3,
			ClientSoftwareName:    "kafka-cli",
			ClientSoftwareVersion: "0.1",
		},
	}

	message := kafkaapi.EncodeApiVersionsRequest(&request)

	err := broker.Send(message)
	if err != nil {
		return err
	}
	response, err := broker.ReceiveRaw()
	if err != nil {
		return err
	}

	decoder := decoder.RealDecoder{}
	decoder.Init(response)

	_, err = decoder.GetInt32()
	if err != nil {
		return fmt.Errorf("failed to decode message length in response: %w", err)
	}

	responseCorrelationId, err := decoder.GetInt32()
	if err != nil {
		return fmt.Errorf("failed to decode correlation_id in response: %w", err)
	}

	if responseCorrelationId != int32(correlationId) {
		return fmt.Errorf("correlation_id in response does not match: %v", responseCorrelationId)
	}

	logger.Successf("✓ Correlation ID: %v", responseCorrelationId)

	return nil
}
