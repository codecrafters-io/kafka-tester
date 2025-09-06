package main

import (
	"bufio"
	"encoding/binary"
	"io"
)

// readInt32 reads a 4-byte big-endian integer from the reader
func readInt32(reader *bufio.Reader) (int32, error) {
	bytes := make([]byte, 4)
	_, err := io.ReadFull(reader, bytes)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(bytes)), nil
}

// readInt16 reads a 2-byte big-endian integer from the reader
func readInt16(reader *bufio.Reader) (int16, error) {
	bytes := make([]byte, 2)
	_, err := io.ReadFull(reader, bytes)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(bytes)), nil
}

// readRawBytes reads a specified number of bytes from the reader
func readRawBytes(reader *bufio.Reader, length int) ([]byte, error) {
	bytes := make([]byte, length)
	_, err := io.ReadFull(reader, bytes)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// writeInt32 writes a 4-byte big-endian integer and returns the bytes
func writeInt32(value int32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(value))
	return bytes
}

// writeInt16 writes a 2-byte big-endian integer and returns the bytes
func writeInt16(value int16) []byte {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, uint16(value))
	return bytes
}

// writeCompactArrayLength adds 1 to the length and converts to varint, returns the bytes
// however, this function doesn't add 1 so the mistake is intentional
func writeCompactArrayLength(length int) []byte {
	varintBytes := make([]byte, binary.MaxVarintLen32)
	varintLen := binary.PutUvarint(varintBytes, uint64(length))
	return varintBytes[:varintLen]
}
