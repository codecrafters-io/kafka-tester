package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
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
	logger.Infof("Sending \"ApiVersions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)

	err := broker.Send(message)
	if err != nil {
		return err
	}
	logger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", GetFormattedHexdump(message))

	response, err := broker.ReceiveRaw()
	if err != nil {
		return err
	}
	logger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", GetFormattedHexdump(response))

	decoder := decoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")

	logger.Debugf("- .Response")
	messageLength, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("message length").WithAddedContext("response")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}
	protocol.LogWithIndentation(logger, 1, "✔️ .message_length (%d)", messageLength)

	logger.Debugf("- .ResponseHeader")
	responseCorrelationId, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("correlation_id").WithAddedContext("response")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}
	protocol.LogWithIndentation(logger, 1, "✔️ .correlation_id (%d)", responseCorrelationId)
	logger.ResetSecondaryPrefix()

	if responseCorrelationId != int32(correlationId) {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", int32(correlationId), responseCorrelationId)
	}

	logger.Successf("✓ Correlation ID: %v", responseCorrelationId)

	return nil
}
