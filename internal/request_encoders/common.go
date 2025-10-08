package request_encoders

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/internal/field_tree_printer"
	protocol_encoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_interface"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

func Encode(request kafka_interface.RequestI, logger *logger.Logger) []byte {
	requestHeader := request.GetHeader()
	apiName := utils.APIKeyToName(requestHeader.ApiKey.Value)

	requestEncoder := field_encoder.NewFieldEncoder()
	requestEncoder.PushPathContext(fmt.Sprintf("%s%s", apiName, "Request"))
	defer requestEncoder.PopPathContext()

	encodeHeader(request.GetHeader(), requestEncoder)

	// Encode the request body using the field encoder
	switch req := request.(type) {
	case kafkaapi.ApiVersionsRequest:
		encodeApiVersionsRequestBody(req.Body, requestEncoder)
	case kafkaapi.DescribeTopicPartitionsRequest:
		encodeDescribeTopicPartitionsRequestBody(req.Body, requestEncoder)
	case kafkaapi.FetchRequest:
		encodeFetchRequestBody(req.Body, requestEncoder)
	case kafkaapi.ProduceRequest:
		encodeProduceRequestBody(req.Body, requestEncoder)
	default:
		panic(fmt.Sprintf("Codecrafters Internal Error - Body encoder not implemented for %s request", apiName))
	}

	// Print encoded values tree
	printEncodedTree(requestEncoder, logger)
	return protocol_encoder.PackEncodedBytesAsMessage(requestEncoder.Bytes())
}

func encodeHeader(header headers.RequestHeader, encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Header")
	defer encoder.PopPathContext()
	encoder.WriteInt16Field("APIKey", header.ApiKey)
	encoder.WriteInt16Field("APIVersion", header.ApiVersion)
	encoder.WriteInt32Field("CorrelationID", header.CorrelationId)
	encoder.WriteStringField("ClientID", header.ClientId)
	encoder.WriteEmptyTagBuffer()
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

func encodeCompactArray[T any](array []T, encoder *field_encoder.FieldEncoder, path string, encodeFunc func(T, *field_encoder.FieldEncoder)) {
	encoder.PushPathContext(path)
	defer encoder.PopPathContext()

	encoder.WriteCompactArrayLengthField("Length", value.NewCompactArrayLength(array))

	for i, element := range array {
		encoder.PushPathContext(fmt.Sprintf("%s[%d]", path, i))
		encodeFunc(element, encoder)
		encoder.PopPathContext()
	}
}

func encodeArray[T any](array []T, encoder *field_encoder.FieldEncoder, path string, encodeFunc func(T, *field_encoder.FieldEncoder)) {
	encoder.PushPathContext(path)
	defer encoder.PopPathContext()

	encoder.WriteInt32Field("Length", value.Int32{Value: int32(len(array))})

	for i, element := range array {
		encoder.PushPathContext(fmt.Sprintf("%s[%d]", path, i))
		encodeFunc(element, encoder)
		encoder.PopPathContext()
	}
}
