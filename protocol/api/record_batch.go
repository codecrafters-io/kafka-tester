package kafkaapi

import (
	"fmt"
	"hash/crc32"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}

func (rb *RecordBatch) Encode(pe *encoder.RealEncoder) {
	startOffset := pe.Offset()

	pe.PutInt64(rb.BaseOffset)
	batchLengthStartOffset := pe.Offset()
	pe.PutInt32(0) // BatchLength placeholder
	pe.PutInt32(rb.PartitionLeaderEpoch)
	pe.PutInt8(2) // Magic value is 2
	crcStartOffset := pe.Offset()
	pe.PutInt32(0) // CRC placeholder
	crcEndOffset := pe.Offset()
	pe.PutInt16(rb.Attributes)
	pe.PutInt32(rb.LastOffsetDelta)
	pe.PutInt64(rb.FirstTimestamp)
	pe.PutInt64(rb.MaxTimestamp)
	pe.PutInt64(rb.ProducerId)
	pe.PutInt16(rb.ProducerEpoch)
	pe.PutInt32(rb.BaseSequence)
	pe.PutInt32(int32(len(rb.Records)))
	for i, record := range rb.Records {
		record.OffsetDelta = int32(i) // Offset Deltas are consecutive numerals from 0 to N-1
		// We can set them programmatically as we know the order of the records
		record.Encode(pe)
	}

	batchLength := pe.Offset() - 12 - startOffset // 8 bytes for BaseOffset & 4 bytes for BatchLength
	pe.PutInt32At(int32(batchLength), batchLengthStartOffset, 4)

	crcData := pe.Bytes()[crcEndOffset:pe.Offset()]
	computedChecksum := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	pe.PutInt32At(int32(computedChecksum), crcStartOffset, 4)
}

func (rb *RecordBatch) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	if rb.BaseOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_offset")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .base_offset (%d)", rb.BaseOffset)

	if rb.BatchLength, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("batch_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .batch_length (%d)", rb.BatchLength)

	if rb.PartitionLeaderEpoch, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_leader_epoch")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .partition_leader_epoch (%d)", rb.PartitionLeaderEpoch)

	if rb.Magic, err = pd.GetInt8(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("magic_byte")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .magic_byte (%d)", rb.Magic)

	if rb.CRC, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("crc")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .crc (%d)", rb.CRC)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	// BatchLength / Message Size contains the size of the message excluding the BaseOffset & the BatchLength
	// From the BatchLength, we need to subtract 9 bytes to get the size of the message that is used in computing the CRC
	// The 9 bytes are:
	// - PartitionLeaderEpoch : 4 bytes
	// - MagicByte : 1 byte
	// - CRC : 4 bytes
	data, _ := pd.GetRawBytesFromOffset(int(rb.BatchLength) - 9)
	computedChecksum := crc32.Checksum(data, crcTable)
	// fmt.Printf("CRC-32C checksum: 0x%08x 0x%08x\n", checksum, uint32(rb.CRC))
	if computedChecksum != uint32(rb.CRC) {
		return errors.NewPacketDecodingError(fmt.Sprintf("CRC mismatch: calculated %08x, expected %08x", computedChecksum, uint32(rb.CRC)), "crc")
	}

	if rb.Attributes, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("record_attributes")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .record_attributes (%d)", rb.Attributes)

	if rb.LastOffsetDelta, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("last_offset_delta")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .last_offset_delta (%d)", rb.LastOffsetDelta)

	if rb.FirstTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_timestamp")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .base_timestamp (%d)", rb.FirstTimestamp)

	if rb.MaxTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("max_timestamp")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .max_timestamp (%d)", rb.MaxTimestamp)

	if rb.ProducerId, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producer_id")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .producer_id (%d)", rb.ProducerId)

	if rb.ProducerEpoch, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producer_epoch")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .producer_epoch (%d)", rb.ProducerEpoch)

	if rb.BaseSequence, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_sequence")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .base_sequence (%d)", rb.BaseSequence)

	numRecords, err := pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_records")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .num_records (%d)", numRecords)

	if numRecords < 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("Count of Records cannot be negative: %d", numRecords))
	}

	for i := 0; i < int(numRecords); i++ {
		record := Record{}
		protocol.LogWithIndentation(logger, indentation, "- .Record[%d]", i)
		err := record.Decode(pd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("Record[%d]", i))
			}
			return err
		}
		rb.Records = append(rb.Records, record)
	}

	return nil
}

type Record struct {
	Length         int32
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

func (r *Record) Encode(pe *encoder.RealEncoder) {
	pe.PutVarint(int64(r.GetEncodedLength())) // Length placeholder
	// As this is variable length, we can't use placeholders and update later reliably.
	// We need to have a value, close to the actual value, such that it takes the same space
	// This is an approx value, the actual value will be computed at the end
	pe.PutInt8(r.Attributes)
	pe.PutVarint(r.TimestampDelta)
	pe.PutVarint(int64(r.OffsetDelta))
	if r.Key == nil {
		pe.PutCompactBytes([]byte{})
	} else {
		pe.PutCompactBytes(r.Key)
	}
	pe.PutVarint(int64(len(r.Value)))
	pe.PutRawBytes(r.Value)
	pe.PutVarint(int64(len(r.Headers)))
	for _, header := range r.Headers {
		header.Encode(pe)
	}
}

func (r *Record) GetEncodedLength() int {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 1024))

	encoder.PutInt8(r.Attributes)
	encoder.PutVarint(r.TimestampDelta)
	encoder.PutVarint(int64(r.OffsetDelta))
	if string(r.Key) == "null" {
		encoder.PutCompactBytes([]byte{})
	} else {
		encoder.PutCompactBytes(r.Key)
	}
	encoder.PutVarint(int64(len(r.Value)))
	encoder.PutRawBytes(r.Value)
	encoder.PutVarint(int64(len(r.Headers)))
	for _, header := range r.Headers {
		header.Encode(&encoder)
	}

	return encoder.Offset()
}

func (r *Record) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	length, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("length")
		}
		return err
	}
	r.Length = int32(length)
	protocol.LogWithIndentation(logger, indentation, "- .length (%d)", r.Length)

	if r.Attributes, err = pd.GetInt8(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("attributes")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .attributes (%d)", r.Attributes)

	if r.TimestampDelta, err = pd.GetSignedVarint(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("timestamp_delta")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .timestamp_delta (%d)", r.TimestampDelta)

	offsetDelta, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("offset_delta")
		}
		return err
	}
	r.OffsetDelta = int32(offsetDelta)
	protocol.LogWithIndentation(logger, indentation, "- .offset_delta (%d)", r.OffsetDelta)

	keyLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("key_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .key_length (%d)", keyLength)

	var key []byte
	if keyLength > 0 {
		key, err = pd.GetRawBytes(int(keyLength))
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("key")
			}
			return err
		}
		r.Key = key
	} else {
		r.Key = nil
	}
	protocol.LogWithIndentation(logger, indentation, "- .key (%q)", string(r.Key))

	valueLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .value_length (%d)", valueLength)

	value, err := pd.GetRawBytes(int(valueLength))
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value")
		}
		return err
	}
	r.Value = value
	protocol.LogWithIndentation(logger, indentation, "- .value (%q)", string(r.Value))

	numHeaders, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_headers")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .num_headers (%d)", numHeaders)

	for i := 0; i < int(numHeaders); i++ {
		header := RecordHeader{}
		protocol.LogWithIndentation(logger, indentation, "- .RecordHeader[%d]", i)
		err := header.Decode(pd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("RecordHeader[%d]", i))
			}
			return err
		}
		r.Headers = append(r.Headers, header)
	}

	return nil
}

type RecordHeader struct {
	Key   string
	Value []byte
}

func (rh *RecordHeader) Encode(pe *encoder.RealEncoder) {
	pe.PutVarint(int64(len(rh.Key)))
	pe.PutBytes([]byte(rh.Key))
	pe.PutVarint(int64(len(rh.Value)))
	pe.PutBytes(rh.Value)
}

func (rh *RecordHeader) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	keyLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("key_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .key_length (%d)", keyLength)

	key, err := pd.GetRawBytes(int(keyLength))
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("key")
		}
		return err
	}
	rh.Key = string(key)
	protocol.LogWithIndentation(logger, indentation, "- .key (%s)", rh.Key)

	valueLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .value_length (%d)", valueLength)

	value, err := pd.GetRawBytes(int(valueLength))
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value")
		}
		return err
	}
	rh.Value = value
	protocol.LogWithIndentation(logger, indentation, "- .value (%s)", rh.Value)

	return nil
}
