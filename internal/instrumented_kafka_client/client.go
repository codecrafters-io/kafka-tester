package instrumented_kafka_client

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
)

type InstrumentedKafkaClient struct {
	*kafka_client.Client

	logger *logger.Logger
}

func NewFromAddr(addr string, baseLogger *logger.Logger, clientId string) *InstrumentedKafkaClient {
	logger := baseLogger.Clone()
	logger.PushSecondaryPrefix(clientId)
	return &InstrumentedKafkaClient{
		Client: kafka_client.NewFromAddr(addr, defaultCallbacks(logger)),
		logger: logger,
	}
}

func defaultCallbacks(logger *logger.Logger) kafka_client.KafkaClientCallbacks {
	return kafka_client.KafkaClientCallbacks{
		BeforeMessageSend: func(message []byte, apiName string) {
			logger.Infof("Sending \"%s\" request", apiName)
			logger.Debugf("Hexdump of sent \"%s\" request: \n%v\n", apiName, utils.GetFormattedHexdump(message))
		},
		AfterResponseReceived: func(response kafka_client.Response, apiName string) {
			logger.Debugf("Hexdump of received \"%s\" response: \n%v\n", apiName, utils.GetFormattedHexdump(response.RawBytes))

		},
		BeforeConnectAttempt: func(addr string) {
			logger.Debugf("Connecting to broker at: %s", addr)
		},
		AfterConnected: func(addr string) {
			logger.Debugf("Connection to broker at %s successful", addr)
		},
		AfterConnectRetriesExceeded: func(addr string) {
			logger.Infof("All connection retries to %s failed. Exiting.", addr)
		},
	}
}
