package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func GetDescribeTopicPartition() {
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.Connect(); err != nil {
		panic(err)
	}
	defer broker.Close()

	response, err := DescribeTopicPartition(broker)
	if err != nil {
		panic(err)
	}
	fmt.Printf("\nTopic name: %s <-> Topic ID: %s", response.Topics[0].Name, response.Topics[0].TopicID)
}

func EncodeDescribeTopicPartitionRequest(request *DescribeTopicPartitionRequest) []byte {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	request.Encode(&encoder)
	messageBytes := encoder.PackMessage()

	return messageBytes
}

func DecodeDescribeTopicPartitionHeader(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, error) {
	decoder := decoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	// DescribeTopicPartition always uses Header v0
	if err := responseHeader.DecodeV0(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("Response Header").WithAddedContext("DescribeTopicPartition v3")
		}
		return nil, err
	}

	return &responseHeader, nil
}

// DecodeDescribeTopicPartitionHeaderAndResponse decodes the header and response
// If an error is encountered while decoding, the returned objects are nil
func DecodeDescribeTopicPartitionHeaderAndResponse(response []byte, logger *logger.Logger) (*ResponseHeader, *DescribeTopicPartitionsResponse, error) {
	decoder := decoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV1(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("DescribeTopicPartition v3")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	DescribeTopicPartitionsResponse := DescribeTopicPartitionsResponse{}
	logger.Debugf("- .ResponseBody")
	if err := DescribeTopicPartitionsResponse.Decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("DescribeTopicPartition v3")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &DescribeTopicPartitionsResponse, nil
}

// DescribeTopicPartition returns api version response or error
func DescribeTopicPartition(b *protocol.Broker) (*DescribeTopicPartitionsResponse, error) {
	request := DescribeTopicPartitionRequest{
		Header: RequestHeader{
			ApiKey:        75,
			ApiVersion:    0,
			CorrelationId: 5,
			ClientId:      "adminclient-1",
		},
		Body: DescribeTopicPartitionRequestBody{
			Topics: []TopicName{
				{
					Name: "foo",
				},
			},
			ResponsePartitionLimit: 1,
		},
	}
	message := EncodeDescribeTopicPartitionRequest(&request)

	response, err := b.SendAndReceive(message)
	if err != nil {
		return nil, err
	}

	_, DescribeTopicPartitionsResponse, err := DecodeDescribeTopicPartitionHeaderAndResponse(response, logger.GetLogger(true, ""))
	if err != nil {
		return nil, err
	}

	return DescribeTopicPartitionsResponse, nil
}
