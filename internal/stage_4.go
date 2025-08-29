package internal

import (
	"fmt"

	"github.com/codecrafters-io/tester-utils/logger"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionErrorCase(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer_legacy.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()
	apiVersion := getInvalidAPIVersion()

	client := kafka_client_legacy.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer client.Close()

	request := kafkaapi.ApiVersionsRequest{
		Header: builder.NewRequestHeaderBuilder().WithApiKey(18).WithApiVersion(int16(apiVersion)).WithCorrelationId(correlationId).Build(),
		Body: kafkaapi.ApiVersionsRequestBody{
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

	stageLogger.UpdateLastSecondaryPrefix("Decoder")
	decoder := decoder.Decoder{}
	decoder.Init(response, stageLogger)

	decoder.BeginSubSection("response")

	_, err = decoder.GetInt32("message_length")
	if err != nil {
		return err
	}

	decoder.BeginSubSection("response_header")
	responseCorrelationId, err := decoder.GetInt32("correlation_id")

	if err != nil {
		return err
	}

	errorCode, err := decoder.GetInt16("error_code")
	if err != nil {
		return err
	}

	stageLogger.ResetSecondaryPrefixes()

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
