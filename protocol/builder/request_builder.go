package builder

type RequestBuilder struct {
	requestType string
	// topics      map[string]map[int32][]kafkaapi.RecordBatch // topicName -> partitionIndex -> recordBatches
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
