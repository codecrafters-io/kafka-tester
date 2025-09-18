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
	apiName := utils.APIKeyToName(requestHeader.ApiKey.Value)

	requestEncoder := field_encoder.NewFieldEncoder()
	requestEncoder.PushPathContext(fmt.Sprintf("%s%s", apiName, "Request"))
	defer requestEncoder.PopPathContext()

	encodedHeader := requestHeader.Encode(requestEncoder)
	encodedBody := request.GetEncodedBody()

	// Print encoded values tree
	printEncodedTree(requestEncoder, logger)

	return protocol_encoder.PackEncodedBytesAsMessage(append(encodedHeader, encodedBody...))
}

func printEncodedTree(encoder *field_encoder.FieldEncoder, logger *logger.Logger) {
	fieldTreePrinterLogger := logger.Clone()
	fieldTreePrinterLogger.UpdateLastSecondaryPrefix("Encoder")

	fieldTreePrinter := field_tree_printer.FieldTreePrinter{
		Fields: encoder.EncodedFields(),
		Logger: fieldTreePrinterLogger,
	}

	fieldTreePrinter.PrintForDebugLogs()
}
