package kafkaapi

import (
	realdecoder "github.com/codecrafters-io/kafka-tester/protocol/decoder"
	realencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func EncodeApiVersionsRequest(request *ApiVersionsRequest) []byte {
	encoder := realencoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	request.Header.EncodeV2(&encoder)
	request.Body.Encode(&encoder)
	messageBytes := encoder.PackMessage()

	return messageBytes
}

func DecodeApiVersionsHeader(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, error) {
	decoder := realdecoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	// APIVersions always uses Header v0
	if err := responseHeader.DecodeV0(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("Response Header").WithAddedContext("ApiVersions v3")
		}
		return nil, err
	}

	return &responseHeader, nil
}

// DecodeApiVersionsHeaderAndResponse decodes the header and response
// If an error is encountered while decoding, the returned objects are nil
func DecodeApiVersionsHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, *ApiVersionsResponse, error) {
	decoder := realdecoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV0(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("ApiVersions v3")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	apiVersionsResponse := ApiVersionsResponse{Version: version}
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
