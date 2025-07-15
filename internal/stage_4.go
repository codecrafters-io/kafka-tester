package internal

import (
	"fmt"

	"github.com/codecrafters-io/tester-utils/logger"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionErrorCase(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()
	apiVersion := getInvalidAPIVersion()

	client := kafka_client.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *kafka_client.Client) {
		_ = client.Close()
	}(client)

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
	stageLogger.Infof("Sending \"ApiVersions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", protocol.GetFormattedHexdump(message))

	err = client.Send(message)
	if err != nil {
		return err
	}
	response, err := client.ReceiveRaw()
	if err != nil {
		return err
	}
	stageLogger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", protocol.GetFormattedHexdump(response))

	decoder := decoder.RealDecoder{}
	decoder.Init(response)
	stageLogger.UpdateSecondaryPrefix("Decoder")

	stageLogger.Debugf("- .Response")
	messageLength, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("message length").WithAddedContext("response")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}
	protocol.LogWithIndentation(stageLogger, 1, "- .message_length (%d)", messageLength)

	stageLogger.Debugf("- .ResponseHeader")
	responseCorrelationId, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("correlation_id").WithAddedContext("response")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}
	protocol.LogWithIndentation(stageLogger, 1, "- .correlation_id (%d)", responseCorrelationId)

	errorCode, err := decoder.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("errorCode").WithAddedContext("ApiVersionsResponse")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}
	protocol.LogWithIndentation(stageLogger, 1, "- .error_code (%d)", errorCode)
	stageLogger.ResetSecondaryPrefix()

	if responseCorrelationId != correlationId {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseCorrelationId)
	}

	stageLogger.Successf("✓ Correlation ID: %v", responseCorrelationId)

	if errorCode != 35 {
		return fmt.Errorf("Expected Error code to be 35, got %v", errorCode)
	}

	stageLogger.Successf("✓ Error code: 35 (UNSUPPORTED_VERSION)")

	return nil
}
