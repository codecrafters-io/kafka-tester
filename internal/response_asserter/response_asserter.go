package response_asserter

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/internal/field_tree_printer"
	"github.com/codecrafters-io/kafka-tester/internal/inspectable_hex_dump"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseAsserter[ResponseType any] struct {
	DecodeFunc func(decoder *field_decoder.FieldDecoder) (ResponseType, field_decoder.FieldDecoderError)
	Assertion  response_assertions.ResponseAssertion[ResponseType]
	Logger     *logger.Logger
}

func (a ResponseAsserter[ResponseType]) DecodeAndAssertSingleFields(responsePayload []byte) (ResponseType, error) {
	decoder := field_decoder.NewFieldDecoder(responsePayload)
	actualResponse, decodeError := a.DecodeFunc(decoder)

	var singleFieldAssertionError error
	var singleFieldAssertionErrorPath field_path.FieldPath

	// First, let's assert the decoded values
	for _, decodedField := range decoder.DecodedFields() {
		if err := a.Assertion.AssertSingleField(decodedField); err != nil {
			singleFieldAssertionError = err
			singleFieldAssertionErrorPath = decodedField.Path
			break
		}
	}

	fieldTreePrinterLogger := a.Logger.Clone()
	fieldTreePrinterLogger.PushSecondaryPrefix("Decoder")

	fieldTreePrinter := field_tree_printer.FieldTreePrinter{
		Fields: decoder.DecodedFields(),
		Logger: fieldTreePrinterLogger,
	}

	// TODO: Add tests for this and revive the logic: Will incorporate in a new PR
	//
	// If there are bytes remaining after decoding, we should report this as an error
	// if assertionError == nil && decodeError == nil && decoder.RemainingBytesCount() != 0 {
	// 	decodeError = &field_decoder.FieldDecoderError{
	// 		Message: fmt.Sprintf("unexpected %d bytes found after decoding response", decoder.RemainingBytesCount()),
	// 		Path:    field_path.NewFieldPath("RemainingBytes"), // Used for formatting error message
	// 	}
	// }

	// Let's prefer single-field assertion errors over decode errors since they're more friendly and actionable
	if singleFieldAssertionError != nil {
		fieldTreePrinter.PrintForErrorLogs(singleFieldAssertionErrorPath, "decode error")

		return actualResponse, singleFieldAssertionError
	}

	if decodeError != nil {
		fieldTreePrinter.PrintForErrorLogs(decodeError.Path(), "decode error")

		receivedBytesHexDump := inspectable_hex_dump.NewInspectableHexDump(responsePayload)
		a.Logger.Errorln("Received bytes:")
		a.Logger.Errorln(receivedBytesHexDump.FormatWithHighlightedOffset(decodeError.Offset()))

		return actualResponse, decodeError
	}

	// If there are no decoder/single-field assertion errors, we only print the tree for debug logs
	fieldTreePrinter.PrintForDebugLogs()

	return actualResponse, nil
}

func (a ResponseAsserter[ResponseType]) DecodeAndAssert(responsePayload []byte) (ResponseType, error) {
	actualResponse, err := a.DecodeAndAssertSingleFields(responsePayload)

	if err != nil {
		return actualResponse, err
	}

	return actualResponse, a.Assertion.AssertAcrossFields(actualResponse, a.Logger)
}
