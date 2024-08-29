package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

func Fetch() {
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.Connect(); err != nil {
		panic(err)
	}
	defer broker.Close()

	response, err := fetch(broker, &FetchRequest{
		MaxWaitMS:         500,
		MinBytes:          1,
		MaxBytes:          52428800,
		IsolationLevel:    0,
		FetchSessionID:    0,
		FetchSessionEpoch: 0,
		Topics: []Topic{
			{
				TopicUUID: "a4c85eb9-307c-43ee-98a6-166e6d7a2e91",
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
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("response: %v\n", response)
}

func EncodeFetchRequest(header *RequestHeader, request *FetchRequest) ([]byte, error) {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 1024))

	header.EncodeV2(&encoder)
	request.Encode(&encoder)
	message := encoder.PackMessage()

	return message, nil
}

// func DecodeFetchHeader(response []byte, version int16) (*ResponseHeader, error) {
// 	decoder := decoder.RealDecoder{}
// 	decoder.Init(response)

// 	responseHeader := ResponseHeader{}
// 	if err := responseHeader.Decode(&decoder); err != nil {
// 		return nil, fmt.Errorf("failed to decode header: %w", err)
// 	}

// 	return &responseHeader, nil
// }

// func DecodeFetchHeaderAndResponse(response []byte, version int16) (*ResponseHeader, *FetchResponse, error) {
// 	decoder := decoder.RealDecoder{}
// 	decoder.Init(response)

// 	responseHeader := ResponseHeader{}
// 	if err := responseHeader.Decode(&decoder); err != nil {
// 		return nil, nil, fmt.Errorf("failed to decode header: %w", err)
// 	}

// 	FetchResponse := FetchResponse{Version: version}
// 	if err := FetchResponse.Decode(&decoder, version); err != nil {
// 		return nil, nil, fmt.Errorf("failed to decode response: %w", err)
// 	}

// 	return &responseHeader, &FetchResponse, nil
// }

// Fetch returns api version response or error
func fetch(b *protocol.Broker, request *FetchRequest) ([]byte, error) {
	header := RequestHeader{
		ApiKey:        1,
		ApiVersion:    16,
		CorrelationId: 0,
		ClientId:      "kafka-tester",
	}
	message, err := EncodeFetchRequest(&header, request)
	if err != nil {
		return nil, err
	}

	response, err := b.SendAndReceive(message)
	if err != nil {
		return nil, err
	}

	fmt.Printf("response: %v\n", response)

	// _, FetchResponse, err := DecodeFetchHeaderAndResponse(response, request.Version)
	// if err != nil {
	// 	return nil, err
	// }

	return response, nil
}
