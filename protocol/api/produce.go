package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func EncodeProduceRequest(request *ProduceRequest) []byte {
	encoder := encoder.Encoder{}
	// bytes.Buffer{}
	encoder.Init(make([]byte, 4096))

	request.Header.EncodeV2(&encoder)
	request.Body.Encode(&encoder)
	message := encoder.PackMessage()

	return message
}

func DecodeProduceHeader(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, error) {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV1(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("Produce Response v11")
			return nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, err
	}

	return &responseHeader, nil
}

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
