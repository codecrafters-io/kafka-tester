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

type FeatureLevelRecord struct {
	Name         string
	FeatureLevel int32
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

	jsonObject := map[string]interface{}{}

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
		jsonObject["type"] = "TOPIC_RECORD"
		jsonObject["version"] = p.Version

		stringLength, err := partialDecoder.GetInt8()
		if err != nil {
			return err
		}

		data, err := partialDecoder.GetRawBytes(int(stringLength) - 1)
		if err != nil {
			return err
		}
		jsonObject["name"] = string(data)

		topicId, err := getUUID(&partialDecoder)
		if err != nil {
			return err
		}
		jsonObject["topicId"] = topicId

		// ToDo: Not sure why we need this: [0]
		x, err := partialDecoder.GetRawBytes(1)
		if err != nil {
			return err
		}
		fmt.Printf("x: %d\n", x)

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PRODUCER_IDS_RECORD")
		}
	}
	// jsonData, err := json.Marshal(jsonObject)
	// if err != nil {
	// 	return err
	// }
	// p.Data = json.RawMessage(jsonData)

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
