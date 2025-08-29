package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/builder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/errors_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testHardcodedCorrelationId(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer_legacy.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	client := kafka_client_legacy.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *kafka_client_legacy.Client) {
		_ = client.Close()
	}(client)
	correlationId := int32(7)

	request := kafkaapi_legacy.ApiVersionsRequest{
		Header: builder_legacy.NewRequestHeaderBuilder().BuildApiVersionsRequestHeader(correlationId),
		Body: kafkaapi_legacy.ApiVersionsRequestBody{
			Version:               4,
			ClientSoftwareName:    "kafka-cli",
			ClientSoftwareVersion: "0.1",
		},
	}

	message := request.Encode()
	stageLogger.Infof("Sending \"ApiVersions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", utils.GetFormattedHexdump(message))

	err = client.Send(message)
	if err != nil {
		return err
	}
	response, err := client.ReceiveRaw()
	if err != nil {
		return err
	}
	stageLogger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", utils.GetFormattedHexdump(response))

	decoder := decoder_legacy.Decoder{}
	decoder.Init(response)
	stageLogger.UpdateLastSecondaryPrefix("Decoder")

	stageLogger.Debugf("- .Response")
	messageLength, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors_legacy.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("message length").WithAddedContext("response")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}
	protocol.LogWithIndentation(stageLogger, 1, "- .message_length (%d)", messageLength)

	stageLogger.Debugf("- .response_header")
	responseCorrelationId, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors_legacy.PacketDecodingError); ok {
			err = decodingErr.WithAddedContext("correlation_id").WithAddedContext("response")
			return decoder.FormatDetailedError(err.Error())
		}
		return err
	}
	protocol.LogWithIndentation(stageLogger, 1, "- .correlation_id (%d)", responseCorrelationId)
	stageLogger.ResetSecondaryPrefixes()

	if responseCorrelationId != int32(correlationId) {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", int32(correlationId), responseCorrelationId)
	}

	stageLogger.Successf("✓ Correlation ID: %v", responseCorrelationId)

	return nil
}
