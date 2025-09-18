package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
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

func (p Partition) Encode(pe *field_encoder.FieldEncoder) {
	pe.PushPathContext("Partition")
	defer pe.PopPathContext()
	pe.WriteInt32Field("ID", p.ID)
	pe.WriteInt32Field("CurrentLeaderEpoch", p.CurrentLeaderEpoch)
	pe.WriteInt64Field("FetchOffset", p.FetchOffset)
	pe.WriteInt32Field("LastFetchedOffset", p.LastFetchedOffset)
	pe.WriteInt64Field("LogStartOffset", p.LogStartOffset)
	pe.WriteInt32Field("PartitionMaxBytes", p.PartitionMaxBytes)
	pe.WriteEmptyTagBuffer()
}
