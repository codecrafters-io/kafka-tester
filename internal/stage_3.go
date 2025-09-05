package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/value_storing_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testCorrelationId(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	if err := serializer_legacy.GenerateLogDirs(logger.GetQuietLogger(""), true); err != nil {
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
	correlationId := getRandomCorrelationId()

	request := kafkaapi.ApiVersionsRequest{
		Header: builder.NewRequestHeaderBuilder().BuildApiVersionsRequestHeader(correlationId),
		Body: kafkaapi.ApiVersionsRequestBody{
			Version:               4,
			ClientSoftwareName:    "kafka-cli",
			ClientSoftwareVersion: "0.1",
		},
	}

	message := request.Encode()
	stageLogger.Infof("Sending \"ApiVersions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", utils.GetFormattedHexdump(message))
	err := client.Send(message)

	if err != nil {
		return err
	}

	response, err := client.ReceiveRaw()

	if err != nil {
		return err
	}

	stageLogger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", utils.GetFormattedHexdump(response))

	// TODO[PaulRefactor]: Actually run assertions!
	// assertion := assertions.NewApiVersionsResponseAssertion().WithCorrelationId(correlationId)
	decoder := value_storing_decoder.NewValueStoringDecoder(response)

	decoder.PushLocatorSegment("ApiVersionsResponse")
	defer decoder.PopLocatorSegment()

	_, err = decoder.ReadInt32("MessageLength")

	if err != nil {
		return err
	}

	decoder.PushLocatorSegment("ResponseHeader")
	defer decoder.PopLocatorSegment()

	_, err = decoder.ReadInt32("CorrelationID")

	if err != nil {
		return err
	}

	stageLogger.ResetSecondaryPrefixes()

	return nil
}
