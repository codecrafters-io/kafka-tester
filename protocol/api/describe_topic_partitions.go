package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

// DecodeDescribeTopicPartitionsHeaderAndResponse decodes the header and response
// If an error is encountered while decoding, the returned objects are nil
func DecodeDescribeTopicPartitionsHeaderAndResponse(response []byte, logger *logger.Logger) (*ResponseHeader, *DescribeTopicPartitionsResponse, error) {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV1(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("DescribeTopicPartitions v0")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	DescribeTopicPartitionsResponse := DescribeTopicPartitionsResponse{}
	logger.Debugf("- .ResponseBody")
	if err := DescribeTopicPartitionsResponse.Decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("DescribeTopicPartitions v0")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &DescribeTopicPartitionsResponse, nil
}
