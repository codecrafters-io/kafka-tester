package legacy_kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafkaapi/legacy_headers"

	"github.com/codecrafters-io/kafka-tester/protocol/legacy_errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func DecodeFetchHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*legacy_headers.ResponseHeader, *FetchResponse, error) {
	decoder := legacy_decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

	responseHeader := legacy_headers.ResponseHeader{Version: 1}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.Decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*legacy_errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("Fetch Response v16")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	fetchResponse := FetchResponse{Version: version}
	logger.Debugf("- .ResponseBody")
	if err := fetchResponse.Decode(&decoder, version, logger, 1); err != nil {
		if decodingErr, ok := err.(*legacy_errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("Fetch Response v16")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &fetchResponse, nil
}
