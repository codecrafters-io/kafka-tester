package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type RequestBuilder struct {
	requestType string
	topics      map[string]map[int32][]kafkaapi.RecordBatch // topicName -> partitionIndex -> recordBatches
}

func NewRequestBuilder(requestType string) RequestBuilderI {
	switch requestType {
	case "produce":
		return &ProduceRequestBuilder{topics: make(map[string]map[int32][]kafkaapi.RecordBatch)}
	default:
		panic("Invalid request type")
	}
}

func NewProduceRequestBuilder() ProduceRequestBuilderI {
	return &ProduceRequestBuilder{topics: make(map[string]map[int32][]kafkaapi.RecordBatch)}
}

func (b *RequestBuilder) Build() RequestBodyI {
	switch b.requestType {
	// case "produce":
	// 	return b.BuildProduceRequest()
	// case "fetch":
	// 	return b.buildFetchRequest()
	default:
		panic("Invalid request type")
	}
}

func (b *RequestBuilder) BuildApiVersionsRequest() kafkaapi.ApiVersionsRequestBody {
	return kafkaapi.ApiVersionsRequestBody{
		Version:               4,
		ClientSoftwareName:    "kafka-cli",
		ClientSoftwareVersion: "0.1",
	}
}
