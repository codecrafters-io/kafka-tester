package kafkaapi

import (
	"fmt"
	realdecoder "github.com/codecrafters-io/kafka-tester/protocol/decoder"
	realencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func Fetch() {
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.Connect(); err != nil {
		panic(err)
	}
	defer broker.Close()

	_, err := fetch(broker, &FetchRequestBody{
		MaxWaitMS:         500,
		MinBytes:          1,
		MaxBytes:          52428800,
		IsolationLevel:    0,
		FetchSessionID:    0,
		FetchSessionEpoch: 0,
		Topics: []Topic{
			{
				TopicUUID: "0f62a58e-617b-462f-9161-132a1946d66a",
				Partitions: []Partition{
					{
						ID:                 0,
						CurrentLeaderEpoch: 0,
						FetchOffset:        0,
						LastFetchedOffset:  -1,
						LogStartOffset:     -1,
						PartitionMaxBytes:  1048576,
					},
				},
			},
		},
		ForgottenTopics: []ForgottenTopic{},
		RackID:          "",
	}, logger.GetLogger(true, ""))
	if err != nil {
		panic(err)
	}
}

func EncodeFetchRequest(request *FetchRequest) []byte {
	encoder := realencoder.RealEncoder{}
	// bytes.Buffer{}
	encoder.Init(make([]byte, 4096))

	request.Header.EncodeV2(&encoder)
	request.Body.Encode(&encoder)
	message := encoder.PackMessage()

	return message
}

func DecodeFetchHeader(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, error) {
	decoder := realdecoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV1(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("Response Header").WithAddedContext("Fetch Response v16")
		}
		return nil, err
	}

	return &responseHeader, nil
}

func DecodeFetchHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, *FetchResponse, error) {
	decoder := realdecoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV1(&decoder, logger, 1); err != nil {
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

// Fetch returns api version response or error
func fetch(b *protocol.Broker, requestBody *FetchRequestBody, logger *logger.Logger) ([]byte, error) {
	request := FetchRequest{
		Header: RequestHeader{
			ApiKey:        1,
			ApiVersion:    16,
			CorrelationId: 0,
			ClientId:      "kafka-tester",
		},
		Body: *requestBody,
	}

	message := EncodeFetchRequest(&request)

	response, err := b.SendAndReceive(message)
	if err != nil {
		return nil, err
	}

	protocol.PrintHexdump(response)

	_, fetchResponse, err := DecodeFetchHeaderAndResponse(response, 16, logger)
	if err != nil {
		return nil, err
	}

	for _, topicResponse := range fetchResponse.TopicResponses {
		for _, partitionResponse := range topicResponse.PartitionResponses {
			for _, recordBatch := range partitionResponse.RecordBatches {
				for _, r := range recordBatch.Records {
					fmt.Printf("message: %s\n", r.Value)
				}
			}
		}
	}

	return nil, nil
}
