package response_asserter

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/internal/field_values_printer"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseAsserter[ResponseType any] struct {
	DecodeFunc func(decoder *field_decoder.FieldDecoder) (ResponseType, error)
	Assertion  response_assertions.ResponseAssertion[ResponseType]
	Logger     *logger.Logger
}

func (a ResponseAsserter[ResponseType]) DecodeAndAssert(responsePayload []byte) (ResponseType, error) {
	decoder := field_decoder.NewFieldDecoder(responsePayload)
	actualResponse, decodeError := a.DecodeFunc(decoder)

	var assertionError error
	var assertionErrorPath field_path.FieldPath

	// First, let's assert the decoded values
	for _, decodedField := range decoder.DecodedFields() {
		if err := a.Assertion.AssertSingleField(decodedField); err != nil {
			assertionError = err
			assertionErrorPath = decodedField.Path
		}
	}

	// TODO[PaulRefactor]: Print debug if no errors
	field_values_printer.FieldValuesPrinter{
		AssertionError:     assertionError,
		AssertionErrorPath: assertionErrorPath,
		DecodeError:        decodeError,
		DecodeErrorPath:    decoder.CurrentPath(),
		DecodedFields:      decoder.DecodedFields(),
		Logger:             a.Logger,
	}.Print()

	// Let's prefer assertion errors over decode errors since they're more friendly and actionable
	if assertionError != nil {
		return actualResponse, assertionError
	}

	if decodeError != nil {
		return actualResponse, decodeError
	}

	return actualResponse, a.Assertion.AssertAcrossFields(actualResponse, a.Logger)
}
