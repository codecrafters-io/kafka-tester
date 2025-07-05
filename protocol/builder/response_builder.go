package builder

type ResponseBuilder struct {
	responseType string
}

func NewResponseBuilder(responseType string) *ProduceResponseBuilder {
	switch responseType {
	case "produce":
		return &ProduceResponseBuilder{
			ResponseBuilder: ResponseBuilder{responseType: responseType},
			topics:          make(map[string]map[int32]*PartitionResponseConfig),
		}
	default:
		panic("Unknown response type: " + responseType)
	}
}
