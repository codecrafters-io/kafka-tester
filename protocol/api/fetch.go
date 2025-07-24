package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func DecodeFetchHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*headers.ResponseHeader, *FetchResponse, error) {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

	responseHeader := headers.ResponseHeader{Version: 1}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.Decode(&decoder, logger, 1); err != nil {
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
