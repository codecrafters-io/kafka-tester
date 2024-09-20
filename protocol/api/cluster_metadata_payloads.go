package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
)

//lint:ignore U1000, these are not used in the codebase currently
type payload struct {
	FrameVersion int8
	Type         int8
	Version      int8
	Data         ClusterMetadataPayloadDataRecord
}

type ClusterMetadataPayloadDataRecord interface {
	isPayloadRecord()
}

type BeginTransactionRecord struct {
	Name string
}

func (b *BeginTransactionRecord) isPayloadRecord() {}

type EndTransactionRecord struct {
}

func (b *EndTransactionRecord) isPayloadRecord() {}

type FeatureLevelRecord struct {
	Name         string
	FeatureLevel int16
}

func (f *FeatureLevelRecord) isPayloadRecord() {}

type ZKMigrationStateRecord struct {
	MigrationState int8
}

func (f *ZKMigrationStateRecord) isPayloadRecord() {}

type TopicRecord struct {
	TopicName string
	TopicUUID string
}

func (t *TopicRecord) isPayloadRecord() {}

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
	Directories      []string
}

func (p *PartitionRecord) isPayloadRecord() {}

//lint:ignore U1000, these are not used in the codebase currently
func (p *payload) Decode(data []byte) (err error) {
	partialDecoder := decoder.RealDecoder{}
	partialDecoder.Init(data)

	p.FrameVersion, err = partialDecoder.GetInt8() // Frame Version: 0
	if err != nil {
		return err
	}

	p.Type, err = partialDecoder.GetInt8()
	if err != nil {
		return err
	}

	p.Version, err = partialDecoder.GetInt8()
	if err != nil {
		return err
	}

	switch p.Type {
	case 23:
		beginTransactionRecord := &BeginTransactionRecord{}
		p.Data = beginTransactionRecord

		taggedFieldCount, err := partialDecoder.GetUnsignedVarint()
		if err != nil {
			return err
		}

		for i := 0; i < int(taggedFieldCount); i++ {
			_, err := partialDecoder.GetUnsignedVarint() // tagType
			if err != nil {
				return err
			}
			_, err = partialDecoder.GetUnsignedVarint() // tagLength
			if err != nil {
				return err
			}

			beginTransactionRecord.Name, err = partialDecoder.GetString()
			if err != nil {
				return err
			}
		}

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PARTITION_CHANGE_RECORD")
		}
	case 21:
		zkMigrationStateRecord := &ZKMigrationStateRecord{}
		p.Data = zkMigrationStateRecord

		zkMigrationStateRecord.MigrationState, err = partialDecoder.GetInt8()
		if err != nil {
			return err
		}

		_, err = partialDecoder.GetUnsignedVarint() // taggedFieldCount
		if err != nil {
			return err
		}

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "ZK_MIGRATION_STATE_RECORD")
		}
	case 2:
		topicRecord := &TopicRecord{}
		p.Data = topicRecord

		topicRecord.TopicName, err = partialDecoder.GetString()
		if err != nil {
			return err
		}

		topicRecord.TopicUUID, err = getUUID(&partialDecoder)
		if err != nil {
			return err
		}

		_, err = partialDecoder.GetUnsignedVarint() // taggedFieldCount
		if err != nil {
			return err
		}

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PRODUCER_IDS_RECORD")
		}
	case 12:
		featureLevelRecord := &FeatureLevelRecord{}
		p.Data = featureLevelRecord

		featureLevelRecord.Name, err = partialDecoder.GetString()
		if err != nil {
			return err
		}

		featureLevelRecord.FeatureLevel, err = partialDecoder.GetInt16()
		if err != nil {
			return err
		}

		_, err = partialDecoder.GetUnsignedVarint() // taggedFieldCount
		if err != nil {
			return err
		}

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "FEATURE_LEVEL_RECORD")
		}
	case 24:
		endTransactionRecord := &EndTransactionRecord{}
		p.Data = endTransactionRecord

		_, err = partialDecoder.GetUnsignedVarint() // taggedFieldCount
		if err != nil {
			return err
		}

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "FEATURE_LEVEL_RECORD")
		}

	case 3:
		partitionRecord := &PartitionRecord{}
		p.Data = partitionRecord

		partitionRecord.PartitionID, err = partialDecoder.GetInt32()
		if err != nil {
			return err
		}

		partitionRecord.TopicUUID, err = getUUID(&partialDecoder)
		if err != nil {
			return err
		}

		partitionRecord.Replicas, err = partialDecoder.GetInt32Array()
		if err != nil {
			return err
		}

		partitionRecord.ISReplicas, err = partialDecoder.GetInt32Array()
		if err != nil {
			return err
		}

		partitionRecord.RemovingReplicas, err = partialDecoder.GetInt32Array()
		if err != nil {
			return err
		}

		partitionRecord.AddingReplicas, err = partialDecoder.GetInt32Array()
		if err != nil {
			return err
		}

		partitionRecord.Leader, err = partialDecoder.GetInt32()
		if err != nil {
			return err
		}

		partitionRecord.LeaderEpoch, err = partialDecoder.GetInt32()
		if err != nil {
			return err
		}

		partitionRecord.PartitionEpoch, err = partialDecoder.GetInt32()
		if err != nil {
			return err
		}

		if p.Version >= 1 {
			partitionRecord.Directories, err = partialDecoder.GetStringArray()
			if err != nil {
				return err
			}
		}

		_, err = partialDecoder.GetUnsignedVarint() // taggedFieldCount
		if err != nil {
			return err
		}

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "FEATURE_LEVEL_RECORD")
		}
	}

	return nil
}

//lint:ignore U1000, these are not used in the codebase currently
func getUUID(pd *decoder.RealDecoder) (string, error) {
	topicUUIDBytes, err := pd.GetRawBytes(16)
	if err != nil {
		return "", err
	}
	topicUUID, err := encoder.DecodeUUID(topicUUIDBytes)
	if err != nil {
		return "", err
	}
	return topicUUID, nil
}
