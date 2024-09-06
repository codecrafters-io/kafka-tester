package internal

import (
	"fmt"
	"math"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testCorrelationId(stageHarness *test_case_harness.TestCaseHarness) error {
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

	correlationId := int32(random.RandomInt(-math.MaxInt32, math.MaxInt32))

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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("message length").WithAddedContext("response")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}

	responseCorrelationId, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("correlation_id").WithAddedContext("response")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}

	if responseCorrelationId != int32(correlationId) {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", int32(correlationId), responseCorrelationId)
	}

	logger.Successf("✓ Correlation ID: %v", responseCorrelationId)

	return nil
}
