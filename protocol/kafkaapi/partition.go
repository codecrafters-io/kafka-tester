package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
)

type Partition struct {
	ID                 int32 // partition id
	CurrentLeaderEpoch int32 // current leader epoch
	FetchOffset        int64 // fetch offset
	LastFetchedOffset  int32 // last fetched offset
	LogStartOffset     int64 // log start offset
	PartitionMaxBytes  int32 // max bytes to fetch
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
