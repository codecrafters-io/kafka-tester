package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type ClusterMetadataPayload struct {
	FrameVersion int8
	Type         int8
	Version      int8
	Data         ClusterMetadataPayloadDataRecord
}

type ClusterMetadataPayloadDataRecord interface {
	isPayloadRecord()
	GetEncodedBytes() []byte
}

type FeatureLevelRecord struct {
	Name         string
	FeatureLevel int16
}

func (f *FeatureLevelRecord) isPayloadRecord() {}

func (f *FeatureLevelRecord) GetEncodedBytes() []byte {
	encoder := encoder.NewEncoder()
	encoder.WriteCompactString(f.Name)
	encoder.WriteInt16(f.FeatureLevel)
	encoder.WriteUvarint(0)
	return encoder.Bytes()
}

type TopicRecord struct {
	TopicName string
	TopicUUID string
}

func (t *TopicRecord) isPayloadRecord() {}

func (t *TopicRecord) GetEncodedBytes() []byte {
	encoder := encoder.NewEncoder()
	encoder.WriteCompactString(t.TopicName)
	encoder.WriteUUID(t.TopicUUID)
	encoder.WriteUvarint(0) // taggedFieldCount
	return encoder.Bytes()
}

type PartitionRecord struct {
	PartitionID      int32
	TopicUUID        string
	Replicas         []int32
	ISReplicas       []int32
	RemovingReplicas []int32
	AddingReplicas   []int32
	Leader           int32
	LeaderEpoch      int32
	PartitionEpoch   int32
	DirectoryUUIDs   []string
}

func (p *PartitionRecord) isPayloadRecord() {}

func (p *PartitionRecord) GetEncodedBytes() []byte {
	encoder := encoder.NewEncoder()

	encoder.WriteInt32(p.PartitionID)
	encoder.WriteUUID(p.TopicUUID)
	encoder.WriteCompactArrayOfInt32(p.Replicas)
	encoder.WriteCompactArrayOfInt32(p.ISReplicas)
	encoder.WriteCompactArrayOfInt32(p.RemovingReplicas)
	encoder.WriteCompactArrayOfInt32(p.AddingReplicas)
	encoder.WriteInt32(p.Leader)
	encoder.WriteInt32(p.LeaderEpoch)
	encoder.WriteInt32(p.PartitionEpoch)

	encoder.WriteUvarint(uint64(len(p.DirectoryUUIDs) + 1))

	for _, directoryUUID := range p.DirectoryUUIDs {
		encoder.WriteUUID(directoryUUID)
	}

	encoder.WriteUvarint(0) // tag buffer
	return encoder.Bytes()
}

func (p ClusterMetadataPayload) Encode(encoder *encoder.Encoder) {
	encoder.WriteInt8(p.FrameVersion)
	encoder.WriteInt8(p.Type)
	encoder.WriteInt8(p.Version)
	encoder.WriteRawBytes(p.Data.GetEncodedBytes())
}
