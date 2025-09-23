package response_assertions

import (
	"github.com/codecrafters-io/kafka-tester/internal/field"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ProduceResponsePartitionData struct {
	Id             int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
}

type ProduceResponseTopicData struct {
	Name       string
	Partitions []ProduceResponsePartitionData
}

type ProduceResponseAssertion struct {
	expectedCorrelationID   int32
	expectedThrottleTimeMs  int32
	expectedTopicProperties []ProduceResponseTopicData
}

func NewProduceResponseAssertion() *ProduceResponseAssertion {
	return &ProduceResponseAssertion{}
}

func (a *ProduceResponseAssertion) ExpectCorrelationId(correlationId int32) *ProduceResponseAssertion {
	a.expectedCorrelationID = correlationId
	return a
}

func (a *ProduceResponseAssertion) ExpectThrottleTimeMs(throttleTimeMs int32) *ProduceResponseAssertion {
	a.expectedThrottleTimeMs = throttleTimeMs
	return a
}

func (a *ProduceResponseAssertion) ExpectTopicProperties(topicProperties []ProduceResponseTopicData) *ProduceResponseAssertion {
	a.expectedTopicProperties = topicProperties
	return a
}

func (a *ProduceResponseAssertion) AssertSingleField(field.Field) error {
	return nil
}

func (a *ProduceResponseAssertion) AssertAcrossFields(response kafkaapi.ProduceResponse, logger *logger.Logger) error {
	return nil
}
