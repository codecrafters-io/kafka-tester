package response_assertions

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	int32_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int32"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FetchResponseAssertion struct {
	expectedCorrelationID        int32
	expectedThrottleTimeMs       int32
	expectedSessionId            int32
	expectedTopicUUID            string
	expectedPartitionId          int32
	expectedErrorCodeInBody      int16
	expectedErrorCodeInPartition int16
}

func NewFetchResponseAssertion() *FetchResponseAssertion {
	return &FetchResponseAssertion{}
}

func (a *FetchResponseAssertion) ExpectCorrelationId(expectedCorrelationID int32) *FetchResponseAssertion {
	a.expectedCorrelationID = expectedCorrelationID
	return a
}

func (a *FetchResponseAssertion) ExpectThrottleTimeMs(expectedThrottleTimeMs int32) *FetchResponseAssertion {
	a.expectedThrottleTimeMs = expectedThrottleTimeMs
	return a
}

func (a *FetchResponseAssertion) ExpectSessionId(expectedSessionId int32) *FetchResponseAssertion {
	a.expectedSessionId = expectedSessionId
	return a
}

func (a *FetchResponseAssertion) ExpectErrorCodeInBody(expectedErorrCode int16) *FetchResponseAssertion {
	a.expectedErrorCodeInBody = expectedErorrCode
	return a
}

func (a *FetchResponseAssertion) ExpectErrorCodeInPartition(expectedErorrCode int16) *FetchResponseAssertion {
	a.expectedErrorCodeInPartition = expectedErorrCode
	return a
}

func (a *FetchResponseAssertion) ExpectTopicUUID(expectedTopicUUID string) *FetchResponseAssertion {
	a.expectedTopicUUID = expectedTopicUUID
	return a
}

func (a *FetchResponseAssertion) ExpectPartitionID(expectedPartitionId int32) *FetchResponseAssertion {
	a.expectedPartitionId = int32(expectedPartitionId)
	return a
}

func (a *FetchResponseAssertion) AssertSingleField(field field_decoder.DecodedField) error {
	if field.Path.String() == "FetchResponse.Header.CorrelationID" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationID, field.Value)
	}

	if field.Path.String() == "FetchResponse.Body.SessionID" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationID, field.Value)
	}

	if field.Path.String() == "FetchResponse.Body.ThrottleTimeMS" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationID, field.Value)
	}

	return nil
}

func (a *FetchResponseAssertion) AssertAcrossFields(response kafkaapi.FetchResponse, logger *logger.Logger) error {
	return nil
	logger.Successf("✓ CorrelationID: %d", a.expectedCorrelationID)
	logger.Successf("✓ SessionID: %d", a.expectedSessionId)
	logger.Successf("✓ ErrorCode: %d (%s)", a.expectedErrorCodeInBody, utils.ErrorCodeToName(a.expectedErrorCodeInBody))
	return nil
}
