package main

import (
	"bytes"
	"encoding/binary"
)

// Response

func PackMessage(messageBytes []byte) []byte {
	messageSize := len(messageBytes)

	result := make([]byte, 4+messageSize)
	binary.BigEndian.PutUint32(result[0:4], uint32(messageSize))
	copy(result[4:], messageBytes)
	return result
}

// Response Header

type ResponseHeader struct {
	CorrelationID int32
}

func (h *ResponseHeader) Encode() []byte {
	return writeInt32(h.CorrelationID)
}

// Response

type Response interface {
	Encode() []byte
}

// ApiVersionsResponse

type ApiVersionsResponse struct {
	Header *ResponseHeader
	Body   ApiVersionsResponseBody
}

func (r *ApiVersionsResponse) Encode() []byte {
	return PackMessage(append(r.Header.Encode(), r.Body.Encode()...))
}

type ApiVersionsResponseBody struct {
	ErrorCode      int16
	ApiKeyEntries  []ApiKeyEntry
	ThrottleTimeMS int32
}

func (a *ApiVersionsResponseBody) Encode() []byte {
	var result bytes.Buffer

	// error code
	result.Write(writeInt16(a.ErrorCode))

	// api entry size
	result.Write(writeCompactArrayLength(len(a.ApiKeyEntries)))

	// api entry array
	for _, apiKeyEntry := range a.ApiKeyEntries {
		result.Write(apiKeyEntry.Encode())
	}

	// throttle time
	result.Write(writeInt32(a.ThrottleTimeMS))

	// Tag buffer
	result.WriteByte(0)

	return result.Bytes()
}

type ApiKeyEntry struct {
	ApiKey              int16
	MinSupportedVersion int16
	MaxSupportedVersion int16
}

func (a *ApiKeyEntry) Encode() []byte {
	var result bytes.Buffer

	// API Key (2 bytes)
	result.Write(writeInt16(a.ApiKey))

	// Min Version (2 bytes)
	result.Write(writeInt16(a.MinSupportedVersion))

	// Max Version (2 bytes)
	result.Write(writeInt16(a.MaxSupportedVersion))

	// Tag buffer (1 byte - empty)
	// Intentionally removed
	// result.WriteByte(0)

	return result.Bytes()
}
