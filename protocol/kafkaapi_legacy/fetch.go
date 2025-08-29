package kafkaapi_legacy

import (
	"github.com/codecrafters-io/kafka-tester/protocol/decoder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy/headers_legacy"

	"github.com/codecrafters-io/kafka-tester/protocol/errors_legacy"
	"github.com/codecrafters-io/tester-utils/logger"
)

func DecodeFetchHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*headers_legacy.ResponseHeader, *FetchResponse, error) {
	decoder := decoder_legacy.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

	responseHeader := headers_legacy.ResponseHeader{Version: 1}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.Decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors_legacy.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("Fetch Response v16")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	fetchResponse := FetchResponse{Version: version}
	logger.Debugf("- .ResponseBody")
	if err := fetchResponse.Decode(&decoder, version, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors_legacy.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("Fetch Response v16")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &fetchResponse, nil
}
