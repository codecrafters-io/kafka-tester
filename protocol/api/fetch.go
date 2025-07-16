package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func DecodeFetchHeader(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, error) {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV1(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("Fetch Response v16")
			return nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, err
	}

	return &responseHeader, nil
}

func DecodeFetchHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, *FetchResponse, error) {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV1(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("Fetch Response v16")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	fetchResponse := FetchResponse{Version: version}
	logger.Debugf("- .ResponseBody")
	if err := fetchResponse.Decode(&decoder, version, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("Fetch Response v16")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &fetchResponse, nil
}
