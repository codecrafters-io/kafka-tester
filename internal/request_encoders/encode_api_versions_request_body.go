package request_encoders

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

func encodeApiVersionsRequestBody(requestBody kafkaapi.ApiVersionsRequestBody, encoder *field_encoder.FieldEncoder) {
	if requestBody.Version.Value < 4 {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported API version: %d", requestBody.Version.Value))
	}
	encoder.PushPathContext("Body")
	defer encoder.PopPathContext()

	encoder.WriteCompactStringField("ClientSoftwareName", requestBody.ClientSoftwareName)
	encoder.WriteCompactStringField("ClientSoftwareVersion", requestBody.ClientSoftwareVersion)
	encoder.WriteEmptyTagBuffer()
}
