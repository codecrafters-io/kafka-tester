package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ApiVersionsResponse struct {
	Header headers.ResponseHeader
	Body   ApiVersionsResponseBody
}

type ApiVersionsResponseBody struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ErrorCode contains the top-level error code.
	ErrorCode value.Int16
	// ApiKeys contains the APIs supported by the broker.
	ApiKeys []ApiKeyEntry
	// ThrottleTimeMs contains the duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs value.Int32
}

// ApiKeyEntry contains the APIs supported by the broker.
type ApiKeyEntry struct {
	// ApiKey contains the API index.
	ApiKey value.Int16
	// MinVersion contains the minimum supported version, inclusive.
	MinVersion value.Int16
	// MaxVersion contains the maximum supported version, inclusive.
	MaxVersion value.Int16
}
