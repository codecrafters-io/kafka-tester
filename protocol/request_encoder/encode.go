package request_encoder

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/internal/field_tree_printer"
	protocol_encoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_interface"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
)

func Encode(request kafka_interface.RequestI, logger *logger.Logger) []byte {
	requestHeader := request.GetHeader()
	apiName := utils.APIKeyToName(requestHeader.ApiKey)

	requestEncoder := field_encoder.NewFieldEncoder()
	requestEncoder.PushPathContext(fmt.Sprintf("%s%s", apiName, "Request"))
	defer requestEncoder.PopPathContext()

	request.GetHeader().Encode(requestEncoder)
	request.EncodeBody(requestEncoder)

	// Print encoded values tree
	printEncodedTree(requestEncoder, logger)

	return protocol_encoder.PackEncodedBytesAsMessage(requestEncoder.Bytes())
}

func printEncodedTree(encoder *field_encoder.FieldEncoder, logger *logger.Logger) {
	// Only print encoded tree in case of
	fieldTreePrinterLogger := logger.Clone()
	fieldTreePrinterLogger.UpdateLastSecondaryPrefix("Encoder")

	encodedFields := make([]field_tree_printer.Field, len(encoder.EncodedFields()))
	for i, field := range encoder.EncodedFields() {
		encodedFields[i] = &field
	}

	fieldTreePrinter := field_tree_printer.FieldTreePrinter{
		Fields: encodedFields,
		Logger: fieldTreePrinterLogger,
	}

	fieldTreePrinter.PrintForDebugLogs()
}
