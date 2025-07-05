package builder

type ResponseBuilder struct {
	responseType string
}

func NewResponseBuilder(responseType string) *ResponseBuilder {
	switch responseType {
	case "produce":
		return &ProduceResponseBuilder{responseType: responseType}
	default:
		panic("Unknown response type: " + responseType)
	}
}
