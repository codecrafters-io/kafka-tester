package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol"
	realdecoder "github.com/codecrafters-io/kafka-tester/protocol/decoder"
	realencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func EncodeDescribeTopicPartitionsRequest(request *DescribeTopicPartitionsRequest) []byte {
	encoder := realencoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	request.Encode(&encoder)
	messageBytes := encoder.PackMessage()

	return messageBytes
}

// DecodeDescribeTopicPartitionsHeaderAndResponse decodes the header and response
// If an error is encountered while decoding, the returned objects are nil
func DecodeDescribeTopicPartitionsHeaderAndResponse(response []byte, logger *logger.Logger) (*ResponseHeader, *DescribeTopicPartitionsResponse, error) {
	decoder := realdecoder.RealDecoder{}
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

// DescribeTopicPartitions returns api version response or error
func DescribeTopicPartitions(b *protocol.Broker) (*DescribeTopicPartitionsResponse, error) {
	request := DescribeTopicPartitionsRequest{
		Header: RequestHeader{
			ApiKey:        75,
			ApiVersion:    0,
			CorrelationId: 5,
			ClientId:      "adminclient-1",
		},
		Body: DescribeTopicPartitionsRequestBody{
			Topics: []TopicName{
				{
					Name: "foo",
				},
			},
			ResponsePartitionLimit: 1,
		},
	}
	message := EncodeDescribeTopicPartitionsRequest(&request)

	response, err := b.SendAndReceive(message)
	if err != nil {
		return nil, err
	}

	_, DescribeTopicPartitionsResponse, err := DecodeDescribeTopicPartitionsHeaderAndResponse(response.Payload, logger.GetLogger(true, ""))
	if err != nil {
		return nil, err
	}

	return DescribeTopicPartitionsResponse, nil
}
