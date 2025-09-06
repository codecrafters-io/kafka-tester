package main

import (
	"bufio"
	"fmt"
)

// Request
type Request struct {
	MessageSize int32
	Header      *RequestHeader
	// Body        RequestBody (Not needed in base stages)
}

// Request Header

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientId      string
}

// We don't need Request body (for base stages)

// ParseRequest reads and parses a Kafka request from the connection
func ParseRequest(reader *bufio.Reader) (*Request, error) {
	messageSize, err := readMessageSize(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}

	requestHeader, err := parseRequestHeader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse request header: %w", err)
	}

	// Parse request body based on the header, but it's not needed in base stages

	return &Request{
		MessageSize: messageSize,
		Header:      requestHeader,
	}, nil
}

func readMessageSize(reader *bufio.Reader) (int32, error) {
	return readInt32(reader)
}

func parseRequestHeader(reader *bufio.Reader) (header *RequestHeader, err error) {
	header = &RequestHeader{}
	// api key
	if header.ApiKey, err = readInt16(reader); err != nil {
		return nil, err
	}

	// api version
	if header.ApiVersion, err = readInt16(reader); err != nil {
		return nil, err
	}

	// correlation ID
	if header.CorrelationID, err = readInt32(reader); err != nil {
		return nil, err
	}

	// client ID length
	var clientIdLength int16
	if clientIdLength, err = readInt16(reader); err != nil {
		return nil, err
	}

	// client ID
	if clientIdLength > 0 {
		clientIDBytes, err := readRawBytes(reader, int(clientIdLength))
		if err != nil {
			return nil, err
		}
		header.ClientId = string(clientIDBytes)
	}

	if _, err = readRawBytes(reader, 1); err != nil {
		return nil, err
	}

	return header, nil
}

func EncodeResponse(response Response) []byte {
	return response.Encode()
}
