package main

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

func main() {
	correlationId := int32(7)

	stageLogger := logger.GetLogger(true, "[test] ")
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.Connect(); err != nil {
		stageLogger.Errorf("Failed to connect to broker: %v", err)
		return
	}

	request := getCT1(correlationId)

	message := kafkaapi.EncodeCreateTopicRequest(&request)
	stageLogger.Infof("Sending \"CreateTopic\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"CreateTopic\" request: \n%v\n", GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message)
	if err != nil {
		stageLogger.Errorf("Failed to send and receive \"CreateTopic\" request: %v", err)
		return
	}
	stageLogger.Debugf("Hexdump of received \"CreateTopic\" response: \n%v\n", GetFormattedHexdump(response.RawBytes))

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

func GetFormattedHexdump(data []byte) string {
	// This is used for logs
	// Contains headers + vertical & horizontal separators + offset
	// We use a different format for the error logs
	var formattedHexdump strings.Builder
	var asciiChars strings.Builder

	formattedHexdump.WriteString("Idx  | Hex                                             | ASCII\n")
	formattedHexdump.WriteString("-----+-------------------------------------------------+-----------------\n")

	for i, b := range data {
		if i%16 == 0 && i != 0 {
			formattedHexdump.WriteString("| " + asciiChars.String() + "\n")
			asciiChars.Reset()
		}
		if i%16 == 0 {
			formattedHexdump.WriteString(fmt.Sprintf("%04x | ", i))
		}
		formattedHexdump.WriteString(fmt.Sprintf("%02x ", b))

		// Add ASCII representation
		if b >= 32 && b <= 126 {
			asciiChars.WriteByte(b)
		} else {
			asciiChars.WriteByte('.')
		}
	}

	// Pad the last line if necessary
	if len(data)%16 != 0 {
		padding := 16 - (len(data) % 16)
		for i := 0; i < padding; i++ {
			formattedHexdump.WriteString("   ")
		}
	}

	// Add the final ASCII representation
	formattedHexdump.WriteString("| " + asciiChars.String())

	return formattedHexdump.String()
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
					Name:              "test-topic",
					NumPartitions:     1,
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
