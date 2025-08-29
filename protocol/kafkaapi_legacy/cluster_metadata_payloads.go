package kafkaapi_legacy

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/errors_legacy"
)

type ClusterMetadataPayload struct {
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
func (p *ClusterMetadataPayload) Decode(data []byte) (err error) {
	partialDecoder := decoder_legacy.Decoder{}
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

			beginTransactionRecord.Name, err = partialDecoder.GetCompactString()
			if err != nil {
				return err
			}
		}

		if partialDecoder.Remaining() > 0 {
			return errors_legacy.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "BEGIN_TRANSACTION_RECORD")
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
			return errors_legacy.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "ZK_MIGRATION_STATE_RECORD")
		}
	case 2:
		topicRecord := &TopicRecord{}
		p.Data = topicRecord

		topicRecord.TopicName, err = partialDecoder.GetCompactString()
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
			return errors_legacy.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "TOPIC_RECORD")
		}
	case 12:
		featureLevelRecord := &FeatureLevelRecord{}
		p.Data = featureLevelRecord

		featureLevelRecord.Name, err = partialDecoder.GetCompactString()
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
			return errors_legacy.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "FEATURE_LEVEL_RECORD")
		}
	case 24:
		endTransactionRecord := &EndTransactionRecord{}
		p.Data = endTransactionRecord

		_, err = partialDecoder.GetUnsignedVarint() // taggedFieldCount
		if err != nil {
			return err
		}

		if partialDecoder.Remaining() > 0 {
			return errors_legacy.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "END_TRANSACTION_RECORD")
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

		partitionRecord.Replicas, err = partialDecoder.GetCompactInt32Array()
		if err != nil {
			return err
		}

		partitionRecord.ISReplicas, err = partialDecoder.GetCompactInt32Array()
		if err != nil {
			return err
		}

		partitionRecord.RemovingReplicas, err = partialDecoder.GetCompactInt32Array()
		if err != nil {
			return err
		}

		partitionRecord.AddingReplicas, err = partialDecoder.GetCompactInt32Array()
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
			arrayLength, err := partialDecoder.GetCompactArrayLength()
			if err != nil {
				return err
			}

			partitionRecord.Directories = make([]string, arrayLength)
			for i := range partitionRecord.Directories {
				partitionRecord.Directories[i], err = getUUID(&partialDecoder)
				if err != nil {
					return err
				}
			}
		}

		_, err = partialDecoder.GetUnsignedVarint() // taggedFieldCount
		if err != nil {
			return err
		}

		if partialDecoder.Remaining() > 0 {
			return errors_legacy.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PARTITION_RECORD")
		}
	}

	return nil
}

func (p ClusterMetadataPayload) Encode(pe *encoder_legacy.Encoder) {
	pe.PutInt8(p.FrameVersion)
	pe.PutInt8(p.Type)
	pe.PutInt8(p.Version)
	switch p.Data.(type) {
	case *BeginTransactionRecord:
		// This record is a bit weird
		// The name is a string, stored inside a tagged field
		// We only expect the Name field and nothing else, so
		// we can hardcode the other values inside the encoder
		record := p.Data.(*BeginTransactionRecord)

		pe.PutUVarint(1)                            // taggedFieldCount
		pe.PutUVarint(0)                            // tagType
		pe.PutUVarint(uint64(len(record.Name)) + 1) // tagLength
		pe.PutCompactString(record.Name)

	case *EndTransactionRecord:
		// This record is empty
		pe.PutUVarint(0) // taggedFieldCount

	case *FeatureLevelRecord:
		record := p.Data.(*FeatureLevelRecord)

		pe.PutCompactString(record.Name)
		pe.PutInt16(record.FeatureLevel)
		pe.PutUVarint(0) // taggedFieldCount

	case *ZKMigrationStateRecord:
		record := p.Data.(*ZKMigrationStateRecord)

		pe.PutInt8(record.MigrationState)
		pe.PutUVarint(0) // taggedFieldCount

	case *TopicRecord:
		record := p.Data.(*TopicRecord)

		pe.PutCompactString(record.TopicName)
		uuidBytes, err := encoder_legacy.EncodeUUID(record.TopicUUID)
		if err != nil {
			panic(err)
		}
		pe.PutRawBytes(uuidBytes)
		pe.PutUVarint(0) // taggedFieldCount

	case *PartitionRecord:
		record := p.Data.(*PartitionRecord)

		pe.PutInt32(record.PartitionID)
		uuidBytes, err := encoder_legacy.EncodeUUID(record.TopicUUID)
		if err != nil {
			panic(err)
		}
		pe.PutRawBytes(uuidBytes)
		pe.PutCompactInt32Array(record.Replicas)
		pe.PutCompactInt32Array(record.ISReplicas)
		pe.PutCompactInt32Array(record.RemovingReplicas)
		pe.PutCompactInt32Array(record.AddingReplicas)
		pe.PutInt32(record.Leader)
		pe.PutInt32(record.LeaderEpoch)
		pe.PutInt32(record.PartitionEpoch)
		if p.Version >= 1 {
			pe.PutUVarint(uint64(len(record.Directories) + 1))
			for i := range record.Directories {
				uuidBytes, err := encoder_legacy.EncodeUUID(record.Directories[i])
				if err != nil {
					panic(err)
				}
				pe.PutRawBytes(uuidBytes)
			}
		}
		pe.PutUVarint(0) // taggedFieldCount
	}
}

//lint:ignore U1000, these are not used in the codebase currently
func getUUID(pd *decoder_legacy.Decoder) (string, error) {
	topicUUIDBytes, err := pd.GetRawBytes(16)
	if err != nil {
		return "", err
	}
	topicUUID, err := encoder_legacy.DecodeUUID(topicUUIDBytes)
	if err != nil {
		return "", err
	}
	return topicUUID, nil
}
