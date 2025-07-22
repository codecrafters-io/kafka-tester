package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

// TODO: Use an universal DecodeHeaderAndResponse function
// Accept an API name, and based on it switch the decoder

func DecodeProduceHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, *ProduceResponseBody, error) {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV1(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("Produce Response v11")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	produceResponse := ProduceResponseBody{Version: version}
	logger.Debugf("- .ResponseBody")
	if err := produceResponse.Decode(&decoder, version, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("Produce Response v11")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &produceResponse, nil
}
