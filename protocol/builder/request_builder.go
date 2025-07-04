package builder

import kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"

type RequestBuilder struct {
	requestType string
}

func NewRequestBuilder(requestType string) *RequestBuilder {
	return &RequestBuilder{
		requestType: requestType,
	}
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

func (b *RequestBuilder) buildProduceRequest(topic_name string) RequestBodyI {
	requestBody := &kafkaapi.ProduceRequestBody{
		TransactionalID: "", // Empty string for non-transactional
		Acks:            1,  // Wait for leader acknowledgment
		TimeoutMs:       5000,
		Topics: []kafkaapi.TopicData{
			{
				Name: topic_name, // Try to produce to a test topic
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
