package kafka_client

import "github.com/codecrafters-io/tester-utils/logger"

// SpawnMultipleClients creates multiple clients and connects them all to the broker
func SpawnMultipleClients(count int, addr string, logger *logger.Logger) []*Client {
	clients := make([]*Client, count)

	for i := range count {
		clients[i] = NewClient(addr)
	}

	return clients
}
