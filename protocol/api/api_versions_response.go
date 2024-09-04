package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("apiKey").WithAddedContext("ApiVersionsResponseKey")
		}
		return err
	}

	if a.MinVersion, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("minVersion").WithAddedContext("ApiVersionsResponseKey")
		}
		return err
	}

	if a.MaxVersion, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("maxVersion").WithAddedContext("ApiVersionsResponseKey")
		}
		return err
	}

	if version >= 3 {
		if _, err := pd.GetEmptyTaggedFieldArray(); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("ApiVersionsResponseKey")
			}
			return err
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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("errorCode").WithAddedContext("ApiVersionsResponse")
		}
		return err
	}

	var numApiKeys int
	if r.Version >= 3 {
		numApiKeys, err = pd.GetCompactArrayLength()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("numApiKeys").WithAddedContext("ApiVersionsResponse")
			}
			return err
		}
	} else {
		numApiKeys, err = pd.GetArrayLength()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("numApiKeys").WithAddedContext("ApiVersionsResponse")
			}
			return err
		}
	}

	r.ApiKeys = make([]ApiVersionsResponseKey, numApiKeys)
	for i := 0; i < numApiKeys; i++ {
		var block ApiVersionsResponseKey
		if err = block.Decode(pd, r.Version); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("apiVersionsResponseKey[%d]", i)).WithAddedContext("ApiVersionsResponse")
			}
			return err
		}
		r.ApiKeys[i] = block
	}

	if r.Version >= 1 {
		if r.ThrottleTimeMs, err = pd.GetInt32(); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("throttleTimeMs").WithAddedContext("ApiVersionsResponse")
			}
			return err
		}
	}

	if r.Version >= 3 {
		if _, err = pd.GetEmptyTaggedFieldArray(); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("ApiVersionsResponse")
			}
			return err
		}
	}

	// Check if there are any remaining bytes in the decoder
	if pd.Remaining() != 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("unexpected %d bytes remaining in decoder after decoding ApiVersionsResponse", pd.Remaining()), "ApiVersionsResponse")
	}

	return nil
}
