package response_asserter

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/internal/field_tree_printer"
	"github.com/codecrafters-io/kafka-tester/internal/inspectable_hex_dump"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseAsserter[ResponseType any] struct {
	DecodeFunc                   func(decoder *field_decoder.FieldDecoder) (ResponseType, field_decoder.FieldDecoderError)
	Assertion                    response_assertions.ResponseAssertion[ResponseType]
	Logger                       *logger.Logger
	IgnoreMessageLengthAssertion bool
}

func (a ResponseAsserter[ResponseType]) DecodeAndAssertSingleFields(response kafka_client.Response) (ResponseType, error) {

	if !a.IgnoreMessageLengthAssertion {
		if err := a.assertMessageLength(response); err != nil {
			var zeroValue ResponseType
			return zeroValue, err
		}
	}

	responsePayload := response.Payload

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
		fieldTreePrinter.PrintForErrorLogs(singleFieldAssertionErrorPath, "value mismatch")

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

func (a ResponseAsserter[ResponseType]) DecodeAndAssert(response kafka_client.Response) (ResponseType, error) {
	actualResponse, err := a.DecodeAndAssertSingleFields(response)

	if err != nil {
		return actualResponse, err
	}

	return actualResponse, a.Assertion.AssertAcrossFields(actualResponse, a.Logger)
}

func (a ResponseAsserter[ResponseType]) assertMessageLength(response kafka_client.Response) error {
	if len(response.RawBytes) < 4 {
		return fmt.Errorf("Response is not long enough to accomodate message size.")
	}

	messageSize := int32(binary.BigEndian.Uint32(response.RawBytes[0:4]))

	if messageSize == int32(len(response.Payload)) {
		return nil
	}

	a.Logger.Errorln("❌ Invalid response:")
	a.Logger.Errorln("The Message Size field does not match the length of the received payload.")

	a.Logger.Errorln("")

	a.Logger.Errorln("🔍 Mismatch:")
	a.Logger.Errorf(
		"Message Size field:\t\t%d (Bytes: %02x %02x %02x %02x)",
		messageSize,
		response.RawBytes[0], response.RawBytes[1], response.RawBytes[2], response.RawBytes[3],
	)
	a.Logger.Errorf("Received payload length:\t%d", len(response.Payload))

	possiblyIncludesMessageSize := messageSize == int32(len(response.Payload)+4)

	if possiblyIncludesMessageSize {
		a.Logger.Errorln("")
		a.Logger.Errorln("💡 Hint:")
		a.Logger.Errorln("The Message Size field should not count itself.")
	}

	return errors.New("")
}
