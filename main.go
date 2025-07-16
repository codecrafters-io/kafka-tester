package main

import (
	"github.com/codecrafters-io/kafka-tester/internal"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

func main() {
	correlationId := int32(7)

	// basePath := common.LOG_DIR

	// err := os.RemoveAll(basePath)
	// if err != nil {
	// 	panic(fmt.Errorf("could not remove log directory at %s: %w", basePath, err))
	// }

	stageLogger := logger.GetLogger(true, "[test] ")
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.Connect(); err != nil {
		stageLogger.Errorf("Failed to connect to broker: %v", err)
		return
	}

	request := getCT1(correlationId)

	message := kafkaapi.EncodeCreateTopicRequest(&request)
	stageLogger.Infof("Sending \"CreateTopic\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"CreateTopic\" request: \n%v\n", internal.GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message)
	if err != nil {
		stageLogger.Errorf("Failed to send and receive \"CreateTopic\" request: %v", err)
		return
	}
	stageLogger.Debugf("Hexdump of received \"CreateTopic\" response: \n%v\n", internal.GetFormattedHexdump(response.RawBytes))

	// For now, just decode the header to verify we get a response
	responseHeader, createTopicResponse, err := kafkaapi.DecodeCreateTopicHeaderAndResponse(response.Payload, 6, stageLogger)
	if err != nil {
		stageLogger.Errorf("Failed to decode \"CreateTopic\" response header: %v", err)
		return
	}

	if responseHeader.CorrelationId != int32(correlationId) {
		stageLogger.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
		return
	}
	stageLogger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)
	stageLogger.Successf("✓ CreateTopic request/response cycle completed!")
	stageLogger.Infof("CreateTopic response: %v", createTopicResponse)
}

func getCT1(correlationId int32) kafkaapi.CreateTopicRequest {
	request := kafkaapi.CreateTopicRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        19, // CreateTopic API
			ApiVersion:    6,  // Use version 6
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.CreateTopicRequestBody{
			Topics: []kafkaapi.TopicData{
				{
					Name:              "topic-1",
					NumPartitions:     3,
					ReplicationFactor: 1,
					Assignments:       []kafkaapi.AssignmentData{},
					Configs:           []kafkaapi.ConfigData{},
				},
			},
			TimeoutMs:    5000,
			ValidateOnly: false,
		},
	}

	return request
}
