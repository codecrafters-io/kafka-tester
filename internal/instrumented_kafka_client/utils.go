package instrumented_kafka_client

import (
	"fmt"

	"github.com/codecrafters-io/tester-utils/logger"
)

// SpawnMultipleClients creates multiple clients and connects them all to the broker
func SpawnMultipleClients(count int, addr string, logger *logger.Logger) []*InstrumentedKafkaClient {
	clients := make([]*InstrumentedKafkaClient, count)

	for i := range count {
		clients[i] = NewFromAddr(addr, logger, fmt.Sprintf("client-%d", i+1))
	}

	return clients
}
