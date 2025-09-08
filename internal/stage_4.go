package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_serializer"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionErrorCase(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	if err := legacy_serializer.GenerateLogDirs(logger.GetQuietLogger(""), true); err != nil {
		return err
	}

	b := kafka_executable.NewKafkaExecutable(stageHarness)

	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()
	apiVersion := getInvalidAPIVersion()
	client := kafka_client.NewClient("localhost:9092")

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

	if err := client.Send(request, stageLogger); err != nil {
		return err
	}

	response, err := client.ReceiveRaw()

	if err != nil {
		return err
	}

	stageLogger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", utils.GetFormattedHexdump(response))
	decoder := decoder.NewDecoder(response, stageLogger)
	decoder.BeginSubSection("ApiVersionsResponse")
	_, err = decoder.ReadInt32("MessageLength")

	if err != nil {
		return err
	}

	decoder.BeginSubSection("ResponseHeader")
	responseCorrelationId, err := decoder.ReadInt32("CorrelationID")

	if err != nil {
		return err
	}

	decoder.EndCurrentSubSection()

	decoder.BeginSubSection("ApiVersionsResponseBody")
	errorCode, err := decoder.ReadInt16("ErrorCode")

	if err != nil {
		return err
	}

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
