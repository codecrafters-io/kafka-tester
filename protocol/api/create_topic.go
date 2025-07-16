package kafkaapi

import (
	realdecoder "github.com/codecrafters-io/kafka-tester/protocol/decoder"
	realencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func EncodeCreateTopicRequest(request *CreateTopicRequest) []byte {
	encoder := realencoder.RealEncoder{}
	// bytes.Buffer{}
	encoder.Init(make([]byte, 4096))

	request.Header.EncodeV2(&encoder)
	request.Body.Encode(&encoder)
	message := encoder.PackMessage()

	return message
}

func DecodeProduceHeader(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, error) {
	decoder := realdecoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefixes([]string{"Decoder"})
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

func DecodeCreateTopicHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, *CreateTopicResponseBody, error) {
	decoder := realdecoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefixes([]string{"Decoder"})
	defer logger.ResetSecondaryPrefixes()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV1(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("CreateTopic Response v6")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	createTopicResponse := CreateTopicResponseBody{}
	logger.Debugf("- .CreateTopicResponseBody")
	if err := createTopicResponse.Decode(&decoder, version, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("CreateTopic Response Body").WithAddedContext("CreateTopic Response v6")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &createTopicResponse, nil
}
