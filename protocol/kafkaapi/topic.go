package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Topic struct {
	UUID       value.UUID
	Partitions []Partition
}
