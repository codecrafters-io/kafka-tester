package builder

func NewResponseBuilder(responseType string) ResponseBuilderI {
	switch responseType {
	case "produce":
		return &ProduceResponseBuilder{
			responseType: responseType,
			topics:       make(map[string]map[int32]*PartitionResponseConfig),
		}
	default:
		panic("Unknown response type: " + responseType)
	}
}

func NewProduceResponseBuilder() *ProduceResponseBuilder {
	return &ProduceResponseBuilder{
		responseType: "produce",
		topics:       make(map[string]map[int32]*PartitionResponseConfig),
	}
}
