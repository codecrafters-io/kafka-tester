package main

import (
	"encoding/binary"
)

// FetchResponse structures based on fetch_response.go
type FetchResponse struct {
	Header ResponseHeader
	Body   FetchResponseBody
}

type ResponseHeader struct {
	CorrelationID uint32
}

type FetchResponseBody struct {
	ThrottleTimeMs uint32
	ErrorCode      uint16
	SessionId      uint32
	TopicResponses []TopicResponse
}

type TopicResponse struct {
	UUID               []byte // 16 bytes
	PartitionResponses []PartitionResponse
}

type PartitionResponse struct {
	Id                   uint32
	ErrorCode            uint16
	HighWatermark        uint64
	LastStableOffset     uint64
	LogStartOffset       uint64
	AbortedTransactions  []AbortedTransaction
	RecordBatches        []RecordBatch
	PreferredReadReplica uint32
}

type AbortedTransaction struct {
	ProducerID  uint64
	FirstOffset uint64
}

// CreateFetchResponse creates a fetch response from the request and decoded batches
func CreateFetchResponse(request *FetchRequest, batches []RecordBatch) *FetchResponse {
	// Calculate high watermark (next offset after all batches)
	highWatermark := uint64(0)
	if len(batches) > 0 {
		lastBatch := batches[len(batches)-1]
		highWatermark = uint64(lastBatch.BaseOffset + 1)
	}

	partitionResponse := PartitionResponse{
		Id:                   request.PartitionID,
		ErrorCode:            0, // No error
		HighWatermark:        highWatermark,
		LastStableOffset:     highWatermark,
		LogStartOffset:       0,
		AbortedTransactions:  []AbortedTransaction{}, // Empty array
		RecordBatches:        batches,
		PreferredReadReplica: 0xFFFFFFFF, // -1 (no preferred replica)
	}

	topicResponse := TopicResponse{
		UUID:               request.TopicID,
		PartitionResponses: []PartitionResponse{partitionResponse},
	}

	responseBody := FetchResponseBody{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionId:      request.SessionID,
		TopicResponses: []TopicResponse{topicResponse},
	}

	response := &FetchResponse{
		Header: ResponseHeader{
			CorrelationID: request.Header.CorrelationID,
		},
		Body: responseBody,
	}

	return response
}

// EncodeResponse encodes the fetch response to bytes
func (r *FetchResponse) EncodeResponse() []byte {
	var data []byte

	// Encode header
	data = append(data, encodeUint32(r.Header.CorrelationID)...)
	data = append(data, 0) // Tag buffer

	// Encode body
	data = append(data, encodeUint32(r.Body.ThrottleTimeMs)...)
	data = append(data, encodeUint16(r.Body.ErrorCode)...)
	data = append(data, encodeUint32(r.Body.SessionId)...)

	// Encode topics (compact array)
	data = append(data, encodeCompactArrayLength(len(r.Body.TopicResponses))...)
	for _, topic := range r.Body.TopicResponses {
		data = append(data, topic.UUID...)

		// Encode partitions (compact array)
		data = append(data, encodeCompactArrayLength(len(topic.PartitionResponses))...)
		for _, partition := range topic.PartitionResponses {
			data = append(data, encodeUint32(partition.Id)...)
			data = append(data, encodeUint16(partition.ErrorCode)...)
			data = append(data, encodeUint64(partition.HighWatermark)...)
			data = append(data, encodeUint64(partition.LastStableOffset)...)
			data = append(data, encodeUint64(partition.LogStartOffset)...)

			// Aborted transactions (compact array)
			data = append(data, encodeCompactArrayLength(len(partition.AbortedTransactions))...)
			for _, txn := range partition.AbortedTransactions {
				data = append(data, encodeUint64(txn.ProducerID)...)
				data = append(data, encodeUint64(txn.FirstOffset)...)
			}

			data = append(data, encodeUint32(partition.PreferredReadReplica)...)

			// Encode record batches
			data = append(data, r.encodeRecordBatches(partition.RecordBatches)...)

			data = append(data, 0) // Partition tag buffer
		}

		data = append(data, 0) // Topic tag buffer
	}

	data = append(data, 0) // Body tag buffer

	// Prepend message size
	messageSize := encodeUint32(uint32(len(data)))
	return append(messageSize, data...)
}

// encodeRecordBatches encodes record batches as raw bytes
func (r *FetchResponse) encodeRecordBatches(batches []RecordBatch) []byte {
	// Calculate total size of all batches
	totalSize := 0
	for i := range batches {
		// totalSize += int(batch.BatchLength) + 12 // +12 for BaseOffset and BatchLength fields
		totalSize += i // DELIBERATELY ENCODE BATCH LENGTH WRONG
	}

	// Write compact bytes length
	result := encodeCompactBytesLength(totalSize)

	// Encode each batch
	for _, batch := range batches {
		batchBytes := encodeRecordBatch(batch)
		result = append(result, batchBytes...)
	}

	return result
}

// encodeRecordBatch encodes a single record batch to bytes
func encodeRecordBatch(batch RecordBatch) []byte {
	var data []byte

	// Encode header
	data = append(data, encodeUint64(uint64(batch.BaseOffset))...)
	data = append(data, encodeUint32(uint32(batch.BatchLength))...)
	data = append(data, encodeUint32(uint32(batch.PartitionLeaderEpoch))...)
	data = append(data, byte(batch.Magic))
	data = append(data, encodeUint32(uint32(batch.CRC))...)
	data = append(data, encodeUint16(uint16(batch.Attributes))...)
	data = append(data, encodeUint32(uint32(batch.LastOffsetDelta))...)
	data = append(data, encodeUint64(uint64(batch.FirstTimestamp))...)
	data = append(data, encodeUint64(uint64(batch.MaxTimestamp))...)
	data = append(data, encodeUint64(uint64(batch.ProducerId))...)
	data = append(data, encodeUint16(uint16(batch.ProducerEpoch))...)
	data = append(data, encodeUint32(uint32(batch.BaseSequence))...)
	data = append(data, encodeUint32(uint32(len(batch.Records)))...)

	// Encode records
	for _, record := range batch.Records {
		recordBytes := encodeRecord(record)
		data = append(data, recordBytes...)
	}

	return data
}

// encodeRecord encodes a single record to bytes
func encodeRecord(record Record) []byte {
	var data []byte

	// Encode the record content first
	var content []byte
	content = append(content, byte(record.Attributes))
	content = append(content, encodeVarint(record.TimestampDelta)...)
	content = append(content, encodeVarint(record.OffsetDelta)...)

	// Encode key
	if record.Key == nil {
		content = append(content, encodeVarint(-1)...)
	} else {
		content = append(content, encodeVarint(int64(len(record.Key)))...)
		content = append(content, record.Key...)
	}

	// Encode value
	if record.Value == nil {
		content = append(content, encodeVarint(-1)...)
	} else {
		content = append(content, encodeVarint(int64(len(record.Value)))...)
		content = append(content, record.Value...)
	}

	// Encode headers (should be -1 for empty)
	content = append(content, encodeVarint(-1)...)

	// Prepend length
	data = append(data, encodeVarint(int64(len(content)))...)
	data = append(data, content...)

	return data
}

// Helper encoding functions
func encodeUint32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func encodeUint16(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func encodeCompactArrayLength(length int) []byte {
	return encodeUVarint(uint64(length + 1))
}

func encodeCompactBytesLength(length int) []byte {
	return encodeUVarint(uint64(length + 1))
}

func encodeUVarint(v uint64) []byte {
	var result []byte
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v == 0 {
			result = append(result, b)
			break
		}
		result = append(result, b|0x80)
	}
	return result
}

func encodeVarint(v int64) []byte {
	// ZigZag encode
	encoded := uint64((v << 1) ^ (v >> 63))
	return encodeUVarint(encoded)
}

// SendFetchResponse sends the fetch response back to the client
func SendFetchResponse(request *FetchRequest, batches []RecordBatch) []byte {

	response := CreateFetchResponse(request, batches)

	return response.EncodeResponse()
}
