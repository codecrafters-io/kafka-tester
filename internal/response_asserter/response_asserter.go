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
	var singleFieldAssertionErrorStartOffset int
	var singleFieldAssertionErrorEndOffset int

	// First, let's assert the decoded values
	for _, decodedField := range decoder.DecodedFields() {
		if err := a.Assertion.AssertSingleField(decodedField); err != nil {
			singleFieldAssertionError = err
			singleFieldAssertionErrorPath = decodedField.Path
			singleFieldAssertionErrorStartOffset = decodedField.StartOffset
			singleFieldAssertionErrorEndOffset = decodedField.EndOffset
			break
		}
	}

	fieldTreePrinterLogger := a.Logger.Clone()
	fieldTreePrinterLogger.PushSecondaryPrefix("Decoder")

	fieldTreePrinter := field_tree_printer.FieldTreePrinter{
		Fields: decoder.DecodedFields(),
		Logger: fieldTreePrinterLogger,
	}

	// Let's prefer single-field assertion errors over decode errors since they're more friendly and actionable
	if singleFieldAssertionError != nil {
		fieldTreePrinter.PrintForFieldAssertionError(singleFieldAssertionErrorPath)
		receivedBytesHexDump := inspectable_hex_dump.NewInspectableHexDump(responsePayload)
		a.Logger.Errorln("Received bytes:")
		a.Logger.Errorln(receivedBytesHexDump.FormatWithHighlightedRange(
			singleFieldAssertionErrorStartOffset,
			singleFieldAssertionErrorEndOffset,
		))
		return actualResponse, singleFieldAssertionError
	}

	if decodeError != nil {
		fieldTreePrinter.PrintForDecodeError(decodeError.Path())
		receivedBytesHexDump := inspectable_hex_dump.NewInspectableHexDump(responsePayload)
		a.Logger.Errorln("Received bytes:")
		a.Logger.Errorln(receivedBytesHexDump.FormatWithHighlightedRange(decodeError.StartOffset(), decodeError.EndOffset()))

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
		// Since this is run after client's send/receive part, hex dump will be printed before this
		return fmt.Errorf("Expected 4-byte integer (message size), only found %d bytes", len(response.RawBytes))
	}

	messageSize := int32(binary.BigEndian.Uint32(response.RawBytes[0:4]))

	if messageSize == int32(len(response.Payload)) {
		return nil
	}

	errorMessage := fmt.Sprintf("Expected the first four bytes be %d (message size), got %d", len(response.Payload), messageSize)

	possiblyIncludesMessageSize := messageSize == int32(len(response.Payload)+4)

	if possiblyIncludesMessageSize {
		errorMessage += "\nHint:The Message Size field should not count itself."
	}

	return errors.New(errorMessage)
}
