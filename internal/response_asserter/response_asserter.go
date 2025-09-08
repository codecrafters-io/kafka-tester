package response_asserter

import (
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/kafka-tester/internal/response_tree_printer"
	"github.com/codecrafters-io/kafka-tester/internal/value_storing_decoder"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseAsserter[ResponseType any] struct {
	DecodeFunc func(decoder *value_storing_decoder.ValueStoringDecoder) (ResponseType, error)
	Assertion  response_assertions.ResponseAssertion[ResponseType]
	Logger     *logger.Logger
}

func (a ResponseAsserter[ResponseType]) DecodeAndAssert(responsePayload []byte) (ResponseType, error) {
	decoder := value_storing_decoder.NewValueStoringDecoder(responsePayload)
	actualResponse, decodeError := a.DecodeFunc(decoder)

	var assertionError error
	var assertionErrorLocator string

	// First, let's assert the decoded values
	for decodedValue, locator := range decoder.DecodedValuesWithLocators() {
		if err := a.Assertion.AssertDecodedValue(locator, decodedValue); err != nil {
			assertionError = err
			assertionErrorLocator = locator
		}
	}

	// TODO[PaulRefactor]: Print debug if no errors
	response_tree_printer.ResponseTreePrinter{
		AssertionError:        assertionError,
		AssertionErrorLocator: assertionErrorLocator,
		DecodeError:           decodeError,
		DecodeErrorLocator:    decoder.CurrentLocator(),
		Decoder:               decoder,
		Logger:                a.Logger,
	}.Print()

	// Let's prefer assertion errors over decode errors since they're more friendly and actionable
	if assertionError != nil {
		return actualResponse, assertionError
	}

	if decodeError != nil {
		return actualResponse, decodeError
	}

	return actualResponse, a.Assertion.RunCompositeAssertions(actualResponse, a.Logger)
}
