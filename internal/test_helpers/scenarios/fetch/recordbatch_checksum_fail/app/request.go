package main

import (
	"bufio"
	"encoding/binary"
	"io"
)

// ReadRequest reads a complete request from the connection
// Uses simplified parsing for single topic/partition assumption
func ReadRequest(reader *bufio.Reader) (*FetchRequest, error) {
	// Read message size
	msgSizeBuf := make([]byte, 4)
	_, err := io.ReadFull(reader, msgSizeBuf)
	if err != nil {
		return nil, err
	}

	// Only fetch requeest will be sent
	req := &FetchRequest{}

	// Read header
	headerBuf := make([]byte, 8)
	_, err = io.ReadFull(reader, headerBuf)
	if err != nil {
		return nil, err
	}

	req.Header.APIKey = binary.BigEndian.Uint16(headerBuf[0:2])
	req.Header.APIVersion = binary.BigEndian.Uint16(headerBuf[2:4])
	req.Header.CorrelationID = binary.BigEndian.Uint32(headerBuf[4:8])

	// Read client ID length
	clientIDLenBuf := make([]byte, 2)
	_, err = io.ReadFull(reader, clientIDLenBuf)
	if err != nil {
		return nil, err
	}

	clientIDLen := binary.BigEndian.Uint16(clientIDLenBuf)

	// Read client ID
	req.Header.ClientID = make([]byte, clientIDLen)
	_, err = io.ReadFull(reader, req.Header.ClientID)
	if err != nil {
		return nil, err
	}

	// Read tag buffer
	_, err = reader.ReadByte()
	if err != nil {
		return nil, err
	}

	// Skip fetch request body fields we don't need for this simple implementation
	// Just read enough to get to the topic UUID and partition ID

	// Read MaxWaitMS (4 bytes)
	maxWaitBuf := make([]byte, 4)
	_, err = io.ReadFull(reader, maxWaitBuf)
	if err != nil {
		return nil, err
	}
	req.MaxWaitMS = binary.BigEndian.Uint32(maxWaitBuf)

	// Read MinBytes (4 bytes)
	minBytesBuf := make([]byte, 4)
	_, err = io.ReadFull(reader, minBytesBuf)
	if err != nil {
		return nil, err
	}
	req.MinBytes = binary.BigEndian.Uint32(minBytesBuf)

	// Read MaxBytes (4 bytes)
	maxBytesBuf := make([]byte, 4)
	_, err = io.ReadFull(reader, maxBytesBuf)
	if err != nil {
		return nil, err
	}
	req.MaxBytes = binary.BigEndian.Uint32(maxBytesBuf)

	// Read IsolationLevel (1 byte)
	isolationByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	req.IsolationLevel = isolationByte

	// Read SessionID (4 bytes)
	sessionIDBuf := make([]byte, 4)
	_, err = io.ReadFull(reader, sessionIDBuf)
	if err != nil {
		return nil, err
	}
	req.SessionID = binary.BigEndian.Uint32(sessionIDBuf)

	// Read SessionEpoch (4 bytes)
	sessionEpochBuf := make([]byte, 4)
	_, err = io.ReadFull(reader, sessionEpochBuf)
	if err != nil {
		return nil, err
	}
	req.SessionEpoch = binary.BigEndian.Uint32(sessionEpochBuf)

	// Read topics count (1 byte for compact array)
	_, err = reader.ReadByte()
	if err != nil {
		return nil, err
	}

	// Read topic UUID (16 bytes)
	req.TopicID = make([]byte, 16)
	_, err = io.ReadFull(reader, req.TopicID)
	if err != nil {
		return nil, err
	}

	// Read partitions count (1 byte for compact array)
	_, err = reader.ReadByte()
	if err != nil {
		return nil, err
	}

	// Read partition ID (4 bytes)
	partitionBuf := make([]byte, 4)
	_, err = io.ReadFull(reader, partitionBuf)
	if err != nil {
		return nil, err
	}
	req.PartitionID = binary.BigEndian.Uint32(partitionBuf)

	// Skip current leader epoch (4 bytes)
	_, err = io.ReadFull(reader, make([]byte, 4))
	if err != nil {
		return nil, err
	}

	// Read fetch offset (8 bytes)
	fetchOffsetBuf := make([]byte, 8)
	_, err = io.ReadFull(reader, fetchOffsetBuf)
	if err != nil {
		return nil, err
	}
	req.FetchOffset = binary.BigEndian.Uint64(fetchOffsetBuf)

	// Skip the rest of the request for now

	return req, nil
}
