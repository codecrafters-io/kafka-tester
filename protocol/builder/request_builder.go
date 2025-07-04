package builder

import kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"

type RequestBuilder struct {
	requestType string
	topicName   string
}

func NewRequestBuilder(requestType string) *RequestBuilder {
	return &RequestBuilder{
		requestType: requestType,
	}
}

func (b *RequestBuilder) WithTopic(topicName string) *RequestBuilder {
	b.topicName = topicName
	return b
}

func (b *RequestBuilder) Build() {
	switch b.requestType {
	case "produce":
		b.buildProduceRequest()
	case "fetch":
		b.buildFetchRequest()
	default:
		panic("Invalid request type")
	}
}

func (b *RequestBuilder) buildProduceRequest() RequestBodyI {
	if b.topicName == "" {
		panic("CodeCrafters Internal Error: Topic name is required to build a produce request")
	}

	requestBody := &kafkaapi.ProduceRequestBody{
		TransactionalID: "", // Empty string for non-transactional
		Acks:            1,  // Wait for leader acknowledgment
		TimeoutMs:       0,
		Topics: []kafkaapi.TopicData{
			{
				Name: b.topicName, // Try to produce to a test topic
				Partitions: []kafkaapi.PartitionData{
					{
						Index:   0,
						Records: []kafkaapi.RecordBatch{},
					},
				},
			},
		},
	}

	return requestBody
}

func (b *RequestBuilder) buildFetchRequest() RequestBodyI {
	return nil
}
