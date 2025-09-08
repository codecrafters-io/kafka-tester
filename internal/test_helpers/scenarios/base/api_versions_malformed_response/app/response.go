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

	// INTENTIONAL ERROR: We only write partial bytes, MaxSupportedVersion is missing
	result.Write(writeInt16(a.ApiKeyEntries[0].ApiKey))
	result.Write(writeInt16(a.ApiKeyEntries[0].MinSupportedVersion))
	result.WriteByte(1) // Incomplete bytes for MaxSupportedVersion

	return result.Bytes()
}

type ApiKeyEntry struct {
	ApiKey              int16
	MinSupportedVersion int16
	MaxSupportedVersion int16
}
