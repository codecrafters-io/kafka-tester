package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	realdecoder "github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionErrorCase(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	quietLogger := logger.GetQuietLogger("")
	logger := stageHarness.Logger
	err := serializer.GenerateLogDirs(quietLogger, true)
	if err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()
	apiVersion := getInvalidAPIVersion()

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, logger); err != nil {
		return err
	}
	defer broker.Close()

	request := kafkaapi.ApiVersionsRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        18,
			ApiVersion:    int16(apiVersion),
			CorrelationId: correlationId,
			ClientId:      "kafka-cli",
		},
		Body: kafkaapi.ApiVersionsRequestBody{
			Version:               4,
			ClientSoftwareName:    "kafka-cli",
			ClientSoftwareVersion: "0.1",
		},
	}

	message := kafkaapi.EncodeApiVersionsRequest(&request)
	logger.Infof("Sending \"ApiVersions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	logger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", GetFormattedHexdump(message))

	err = broker.Send(message)
	if err != nil {
		return err
	}
	response, err := broker.ReceiveRaw()
	if err != nil {
		return err
	}
	logger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", GetFormattedHexdump(response))

	decoder := realdecoder.RealDecoder{}
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
	protocol.LogWithIndentation(logger, 1, "- .message_length (%d)", messageLength)

	logger.Debugf("- .ResponseHeader")
	responseCorrelationId, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("correlation_id").WithAddedContext("response")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}
	protocol.LogWithIndentation(logger, 1, "- .correlation_id (%d)", responseCorrelationId)

	errorCode, err := decoder.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("errorCode").WithAddedContext("ApiVersionsResponse")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}
	protocol.LogWithIndentation(logger, 1, "- .error_code (%d)", errorCode)
	logger.ResetSecondaryPrefix()

	if responseCorrelationId != correlationId {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseCorrelationId)
	}

	logger.Successf("✓ Correlation ID: %v", responseCorrelationId)

	if errorCode != 35 {
		return fmt.Errorf("Expected Error code to be 35, got %v", errorCode)
	}

	logger.Successf("✓ Error code: 35 (UNSUPPORTED_VERSION)")

	return nil
}
