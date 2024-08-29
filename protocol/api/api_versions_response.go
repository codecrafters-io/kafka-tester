package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
)

// ApiVersionsResponseKey contains the APIs supported by the broker.
type ApiVersionsResponseKey struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ApiKey contains the API index.
	ApiKey int16
	// MinVersion contains the minimum supported version, inclusive.
	MinVersion int16
	// MaxVersion contains the maximum supported version, inclusive.
	MaxVersion int16
}

func (a *ApiVersionsResponseKey) Decode(pd *decoder.RealDecoder, version int16) (err error) {
	a.Version = version
	if a.ApiKey, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode apiKey in api versions response key: %w", err)
	}

	if a.MinVersion, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode minVersion in api versions response key: %w", err)
	}

	if a.MaxVersion, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode maxVersion in api versions response key: %w", err)
	}

	if version >= 3 {
		if _, err := pd.GetEmptyTaggedFieldArray(); err != nil {
			return fmt.Errorf("failed to decode taggedFieldArray in api versions response key: %w", err)
		}
	}

	return nil
}

type ApiVersionsResponse struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ErrorCode contains the top-level error code.
	ErrorCode int16
	// ApiKeys contains the APIs supported by the broker.
	ApiKeys []ApiVersionsResponseKey
	// ThrottleTimeMs contains the duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32
}

func (r *ApiVersionsResponse) Decode(pd *decoder.RealDecoder, version int16) (err error) {
	r.Version = version
	if r.ErrorCode, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode errorCode in api versions response: %w", err)
	}

	var numApiKeys int
	if r.Version >= 3 {
		numApiKeys, err = pd.GetCompactArrayLength()
		if err != nil {
			return fmt.Errorf("failed to decode numApiKeys in api versions response: %w", err)
		}
	} else {
		numApiKeys, err = pd.GetArrayLength()
		if err != nil {
			return fmt.Errorf("failed to decode numApiKeys in api versions response: %w", err)
		}
	}

	r.ApiKeys = make([]ApiVersionsResponseKey, numApiKeys)
	for i := 0; i < numApiKeys; i++ {
		var block ApiVersionsResponseKey
		if err = block.Decode(pd, r.Version); err != nil {
			return fmt.Errorf("failed to decode api versions response key: %w", err)
		}
		r.ApiKeys[i] = block
	}

	if r.Version >= 1 {
		if r.ThrottleTimeMs, err = pd.GetInt32(); err != nil {
			return fmt.Errorf("failed to decode throttleTimeMs in api versions response: %w", err)
		}
	}

	if r.Version >= 3 {
		if _, err = pd.GetEmptyTaggedFieldArray(); err != nil {
			return fmt.Errorf("failed to decode taggedFieldArray in api versions response: %w", err)
		}
	}

	// Check if there are any remaining bytes in the decoder
	if pd.Remaining() != 0 {
		return fmt.Errorf("unexpected %d bytes remaining in decoder after decoding ApiVersionsResponse", pd.Remaining())
	}

	return nil
}
