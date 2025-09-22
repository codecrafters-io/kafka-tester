package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Partition struct {
	ID                 value.Int32 // partition id
	CurrentLeaderEpoch value.Int32 // current leader epoch
	FetchOffset        value.Int64 // fetch offset
	LastFetchedOffset  value.Int32 // last fetched offset
	LogStartOffset     value.Int64 // log start offset
	PartitionMaxBytes  value.Int32 // max bytes to fetch
}
