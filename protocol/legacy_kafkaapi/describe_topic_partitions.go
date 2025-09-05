package legacy_kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_errors"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafkaapi/legacy_headers"
	"github.com/codecrafters-io/tester-utils/logger"
)

// DecodeDescribeTopicPartitionsHeaderAndResponse decodes the header and response
// If an error is encountered while decoding, the returned objects are nil
func DecodeDescribeTopicPartitionsHeaderAndResponse(response []byte, logger *logger.Logger) (*legacy_headers.ResponseHeader, *DescribeTopicPartitionsResponse, error) {
	decoder := legacy_decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

	responseHeader := legacy_headers.ResponseHeader{Version: 1}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.Decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*legacy_errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("DescribeTopicPartitions v0")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	DescribeTopicPartitionsResponse := DescribeTopicPartitionsResponse{}
	logger.Debugf("- .ResponseBody")
	if err := DescribeTopicPartitionsResponse.Decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*legacy_errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("DescribeTopicPartitions v0")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &DescribeTopicPartitionsResponse, nil
}
