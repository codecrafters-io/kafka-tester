package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/kafka-tester/internal/response_tree_printer"
	"github.com/codecrafters-io/kafka-tester/internal/value_storing_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer_legacy"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersion(stageHarness *test_case_harness.TestCaseHarness) error {
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
	request := builder.NewApiVersionsRequestBuilder().WithCorrelationId(correlationId).Build()
	response, err := client.SendAndReceive(request, stageLogger)

	if err != nil {
		return err
	}

	assertion := response_assertions.NewApiVersionsResponseAssertion().
		WithCorrelationId(correlationId).
		WithErrorCode(0).
		WithApiKeyEntry(18, 0, 4)

	decoder := value_storing_decoder.NewValueStoringDecoder(response.Payload)

	// TODO[PaulRefactor]: This seems like a common pattern that will be in all stages - Decode, followed by RunCompositeAssertions. See if we can make this more easy todo?
	actualResponse, err := kafkaapi.DecodeApiVersionsResponse(decoder)
	return Unnamed(actualResponse, assertion, decoder, err, stageLogger)
}

func Unnamed[T any](actualResponse T, assertion response_assertions.ResponseAssertion[T], decoder *value_storing_decoder.ValueStoringDecoder, decodeError error, stageLogger *logger.Logger) error {
	var assertionError error
	var assertionErrorLocator string

	// First, let's assert the decoded values
	for decodedValue, locator := range decoder.DecodedValuesWithLocators() {
		if err := assertion.AssertDecodedValue(locator, decodedValue); err != nil {
			assertionError = err
			assertionErrorLocator = locator
		}
	}

	responseTreePrinter := response_tree_printer.ResponseTreePrinter{
		AssertionError:        assertionError,
		AssertionErrorLocator: assertionErrorLocator,
		DecodeError:           decodeError,
		DecodeErrorLocator:    decoder.CurrentLocator(),
		Decoder:               decoder,
		Logger:                stageLogger,
	}

	responseTreePrinter.Print()

	if assertionError != nil {
		return assertionError
	}

	if decodeError != nil {
		return decodeError
	}

	return assertion.RunCompositeAssertions(actualResponse, stageLogger)
}
