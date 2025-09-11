package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_handler"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	decoder "github.com/codecrafters-io/kafka-tester/protocol/legacy_decoder_2"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testHardcodedCorrelationId(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	if err := kafka_files_handler.NewFilesHandler(logger.GetQuietLogger("")).GenerateServerConfiguration(); err != nil {
		return err
	}

	b := kafka_executable.NewKafkaExecutable(stageHarness)

	if err := b.Run(); err != nil {
		return err
	}

	client := kafka_client.NewClient("localhost:9092")

	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}

	defer client.Close()
	correlationId := int32(7)

	request := kafkaapi.ApiVersionsRequest{
		Header: builder.NewRequestHeaderBuilder().BuildApiVersionsRequestHeader(correlationId),
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

	stageLogger.ResetSecondaryPrefixes()

	if responseCorrelationId.Value != correlationId {
		return fmt.Errorf("Expected Correlation ID to be %d, got %d", int32(correlationId), responseCorrelationId.Value)
	}

	stageLogger.Successf("✓ Correlation ID: %d", responseCorrelationId.Value)

	return nil
}
