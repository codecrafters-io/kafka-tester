package main

type RequestHeader struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
	ClientID      []byte
}

type FetchRequest struct {
	Header         RequestHeader
	MaxWaitMS      uint32
	MinBytes       uint32
	MaxBytes       uint32
	IsolationLevel uint8
	SessionID      uint32
	SessionEpoch   uint32
	TopicID        []byte // Only one topic (16 bytes UUID)
	PartitionID    uint32 // Only one partition
	FetchOffset    uint64
}

func PrintRequest(req *FetchRequest) {
	// Removed logging
}
