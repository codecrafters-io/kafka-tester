package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/assertions/validations"
	"github.com/codecrafters-io/kafka-tester/internal/assertions/validations/int16_validation"
	"github.com/codecrafters-io/kafka-tester/internal/assertions/validations/int32_validation"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer_legacy"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersion(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	if err := serializer_legacy.GenerateLogDirs(logger.GetQuietLogger(""), true); err != nil {
		return err
	}

	b := kafka_executable.NewKafkaExecutable(stageHarness)

	if err := b.Run(); err != nil {
		return err
	}

	client := kafka_client.NewClient("localhost:9092")

	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}

	defer client.Close()

	correlationId := getRandomCorrelationId()
	request := builder.NewApiVersionsRequestBuilder().WithCorrelationId(correlationId).Build()
	response, err := client.SendAndReceive(request, stageLogger)

	if err != nil {
		return err
	}

	actualResponse := builder.NewApiVersionsResponseBuilder().BuildEmpty()

	assertion := assertions.NewApiVersionsResponseAssertion().SetPrimitiveValidations(
		validations.NewValidationMap(map[string]validations.Validation{
			"ApiVersionsResponse.ResponseHeader.CorrelationID":      int32_validation.IsEqual(correlationId),
			"ApiVersionsResponse.ApiVersionsResponseBody.ErrorCode": int16_validation.IsEqual(0),
		}),
	)
	return actualResponse.Decode(response.Payload, stageLogger, assertion)
}
