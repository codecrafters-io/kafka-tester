package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/internal/field_tree_printer"
	"github.com/codecrafters-io/kafka-tester/internal/inspectable_hex_dump"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/response_decoders"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersion(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	if err := legacy_serializer.GenerateLogDirs(logger.GetQuietLogger(""), true); err != nil {
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
	request := builder.NewApiVersionsRequestBuilder().WithCorrelationId(correlationId).Build()
	rawResponse, err := client.SendAndReceive(request, stageLogger)

	if err != nil {
		return err
	}

	// TODO: Move fieldDecoder construction out of individual stage test files
	fieldDecoder := field_decoder.NewFieldDecoder(rawResponse.Payload)

	decoderLogger := stageLogger.Clone()
	decoderLogger.PushSecondaryPrefix("Decoder")

	actualResponse, decodeErr := response_decoders.DecodeApiVersionsResponse(fieldDecoder)

	if decodeErr != nil {
		// TODO: Move logic for printing field tree out of individual stage test files
		field_tree_printer.FieldTreePrinter{
			DecodedFields: fieldDecoder.DecodedFields(),
			Logger:        decoderLogger,
		}.PrintForErrorLogs(decodeErr.Path())

		// TODO: Move logic for printing hex dump out of individual stage test files
		receivedBytesHexDump := inspectable_hex_dump.NewInspectableHexDump(rawResponse.Payload)
		stageLogger.Errorln("Received bytes:")
		stageLogger.Errorln(receivedBytesHexDump.FormatWithHighlightedOffset(decodeErr.Offset()))

		return decodeErr
	}

	// TODO: Move logic for printing field tree out of individual stage test files
	if decoderLogger.IsDebug {
		field_tree_printer.FieldTreePrinter{
			DecodedFields: fieldDecoder.DecodedFields(),
			Logger:        decoderLogger,
		}.PrintForDebugLogs()
	}

	// TODO: Move away from ResponseBuilder to using only assertions
	expectedApiVersionResponse := builder.NewApiVersionsResponseBuilder().
		AddApiKeyEntry(18, 0, 4).
		WithCorrelationId(correlationId).
		Build()

	// TODO: Integrate FieldTreePrinter into assertion errors
	if err = assertions.NewApiVersionsResponseAssertion(actualResponse, expectedApiVersionResponse).Run(stageLogger); err != nil {
		return err
	}

	return nil
}
