package misc

import (
	"encoding/json"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
)

type payload struct {
	FrameVersion int8
	Type         int8
	Version      int8
	Data         json.RawMessage
}

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
	case 5:
		jsonObject["type"] = "PARTITION_CHANGE_RECORD"
		jsonObject["version"] = p.Version
		partitionId, err := partialDecoder.GetInt32()
		if err != nil {
			return err
		}
		jsonObject["partitionId"] = partitionId

		topicId, err := getUUID(&partialDecoder)
		if err != nil {
			return err
		}
		jsonObject["topicId"] = topicId

		// skip 3 bytes (not sure why) ToDo
		x, err := partialDecoder.GetRawBytes(3)
		if err != nil {
			return err
		}
		fmt.Printf("x: %d\n", x)

		leader, err := partialDecoder.GetInt32()
		if err != nil {
			return err
		}
		jsonObject["leader"] = leader

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PARTITION_CHANGE_RECORD")
		}
	case 20:
		jsonObject["type"] = "NO_OP_RECORD"
		jsonObject["version"] = p.Version

		// COMPACT_NULLABLE_BYTES
		// 0x00 - NULL
		dataLength, err := partialDecoder.GetUnsignedVarint()
		if err != nil {
			return err
		}
		if dataLength == 0 {
			jsonObject["data"] = nil
		} else {
			jsonObject["data"] = make([]interface{}, dataLength)
		}

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PARTITION_CHANGE_RECORD")
		}
	case 17:
		jsonObject["type"] = "BROKER_REGISTRATION_CHANGE_RECORD"
		jsonObject["version"] = p.Version

		brokerId, err := partialDecoder.GetInt32()
		if err != nil {
			return err
		}
		jsonObject["brokerId"] = brokerId

		brokerEpoch, err := partialDecoder.GetInt64()
		if err != nil {
			return err
		}
		jsonObject["brokerEpoch"] = brokerEpoch

		fenced, err := partialDecoder.GetInt8()
		if err != nil {
			return err
		}
		jsonObject["fenced"] = fenced

		// if p.Version == 1 { // seems to be present always
		inControlledShutdown, err := partialDecoder.GetInt8()
		if err != nil {
			return err
		}
		jsonObject["inControlledShutdown"] = inControlledShutdown
		// }

		// ToDo: Not sure why we need this
		x, err := partialDecoder.GetRawBytes(2)
		if err != nil {
			return err
		}
		fmt.Printf("x: %d\n", x)

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PARTITION_CHANGE_RECORD")
		}
	case 23:
		jsonObject["type"] = "BEGIN_TRANSACTION_RECORD"
		jsonObject["version"] = p.Version

		// ToDo: Not sure why we need this
		x, err := partialDecoder.GetRawBytes(3)
		if err != nil {
			return err
		}
		fmt.Printf("x: %d\n", x)

		stringLength, err := partialDecoder.GetInt8()
		if err != nil {
			return err
		}

		data, err := partialDecoder.GetRawBytes(int(stringLength) - 1)
		if err != nil {
			return err
		}
		jsonObject["name"] = string(data)
		fmt.Printf("name: %q\n", jsonObject["name"])

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PARTITION_CHANGE_RECORD")
		}
	case 21:
		jsonObject["type"] = "ZK_MIGRATION_STATE_RECORD"
		jsonObject["version"] = p.Version

		// ToDo: Not sure why we need this (0) also dict
		x, err := partialDecoder.GetRawBytes(1)
		if err != nil {
			return err
		}
		fmt.Printf("x: %d\n", x)

		ZkMigrationState, err := partialDecoder.GetInt8()
		if err != nil {
			return err
		}

		jsonObject["zkMigrationState"] = ZkMigrationState

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "ZK_MIGRATION_STATE_RECORD")
		}
	case 15:
		jsonObject["type"] = "PRODUCER_IDS_RECORD"
		jsonObject["version"] = p.Version

		brokerId, err := partialDecoder.GetInt32()
		if err != nil {
			return err
		}
		jsonObject["brokerId"] = brokerId

		brokerEpoch, err := partialDecoder.GetInt64()
		if err != nil {
			return err
		}
		jsonObject["brokerEpoch"] = brokerEpoch

		nextProducerId, err := partialDecoder.GetInt64()
		if err != nil {
			return err
		}
		jsonObject["nextProducerId"] = nextProducerId

		// ToDo: Not sure why we need this: [0]
		x, err := partialDecoder.GetRawBytes(1)
		if err != nil {
			return err
		}
		fmt.Printf("x: %d\n", x)

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PRODUCER_IDS_RECORD")
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
	jsonData, err := json.Marshal(jsonObject)
	if err != nil {
		return err
	}
	p.Data = json.RawMessage(jsonData)

	return nil
}

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
