package kafkaapi

import "github.com/codecrafters-io/kafka-tester/protocol/encoder"

type Partition struct {
	ID                 int32 // partition id
	CurrentLeaderEpoch int32 // current leader epoch
	FetchOffset        int64 // fetch offset
	LastFetchedOffset  int32 // last fetched offset
	LogStartOffset     int64 // log start offset
	PartitionMaxBytes  int32 // max bytes to fetch
}

func (p Partition) Encode(pe *encoder.Encoder) {
	pe.WriteInt32(p.ID)
	pe.WriteInt32(p.CurrentLeaderEpoch)
	pe.WriteInt64(p.FetchOffset)
	pe.WriteInt32(p.LastFetchedOffset)
	pe.WriteInt64(p.LogStartOffset)
	pe.WriteInt32(p.PartitionMaxBytes)
	pe.WriteEmptyTagBuffer()
}
