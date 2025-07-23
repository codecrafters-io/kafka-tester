package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/tester-utils/logger"
)

func EncodeDescribeTopicPartitionsRequest(request *DescribeTopicPartitionsRequest) []byte {
	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 4096))

	request.Encode(&encoder)
	messageBytes := encoder.PackMessage()

	return messageBytes
}

// DecodeDescribeTopicPartitionsHeaderAndResponse decodes the header and response
// If an error is encountered while decoding, the returned objects are nil
func DecodeDescribeTopicPartitionsHeaderAndResponse(response []byte, logger *logger.Logger) (*ResponseHeader, *DescribeTopicPartitionsResponse, error) {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

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
func DescribeTopicPartitions(c *kafka_client.Client) (*DescribeTopicPartitionsResponse, error) {
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

	response, err := c.SendAndReceive(message)
	if err != nil {
		return nil, err
	}

	_, DescribeTopicPartitionsResponse, err := DecodeDescribeTopicPartitionsHeaderAndResponse(response.Payload, logger.GetLogger(true, ""))
	if err != nil {
		return nil, err
	}

	return DescribeTopicPartitionsResponse, nil
}
