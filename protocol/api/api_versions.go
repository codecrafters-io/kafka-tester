package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func EncodeApiVersionsRequest(r *ApiVersionsRequest) []byte {
	return encoder.PackMessage(append(r.Header.Encode(), r.Body.Encode()...))
}

// DecodeApiVersionsHeaderAndResponse decodes the header and response
// If an error is encountered while decoding, the returned objects are nil
func DecodeApiVersionsHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*headers.ResponseHeader, *ApiVersionsResponseBody, error) {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

	responseHeader := headers.ResponseHeader{Version: 0}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.Decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("ApiVersions v3")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	apiVersionsResponse := ApiVersionsResponseBody{Version: version}
	logger.Debugf("- .ResponseBody")
	if err := apiVersionsResponse.Decode(&decoder, version, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("ApiVersions v3")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &apiVersionsResponse, nil
}
