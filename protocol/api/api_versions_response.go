package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

// ApiKeyEntry contains the APIs supported by the broker.
type ApiKeyEntry struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ApiKey contains the API index.
	ApiKey int16
	// MinVersion contains the minimum supported version, inclusive.
	MinVersion int16
	// MaxVersion contains the maximum supported version, inclusive.
	MaxVersion int16
}

func (a *ApiKeyEntry) Decode(pd *decoder.Decoder, version int16, logger *logger.Logger, indentation int) (err error) {
	a.Version = version
	if a.ApiKey, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("api_key")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .api_key (%d)", a.ApiKey)

	if a.MinVersion, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("min_version")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .min_version (%d)", a.MinVersion)

	if a.MaxVersion, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("max_version")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .max_version (%d)", a.MaxVersion)

	if version >= 3 {
		if _, err := pd.GetEmptyTaggedFieldArray(); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("TAG_BUFFER")
			}
			return err
		}
		protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")
	}

	return nil
}

type ApiVersionsResponseBody struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ErrorCode contains the top-level error code.
	ErrorCode int16
	// ApiKeys contains the APIs supported by the broker.
	ApiKeys []ApiKeyEntry
	// ThrottleTimeMs contains the duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32
}

func (r *ApiVersionsResponseBody) Decode(pd *decoder.Decoder, version int16, logger *logger.Logger, indentation int) (err error) {
	r.Version = version
	if r.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .error_code (%d)", r.ErrorCode)

	var numApiKeys int
	if r.Version >= 3 {
		numApiKeys, err = pd.GetCompactArrayLength()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("num_api_keys")
			}
			return err
		}
	} else {
		numApiKeys, err = pd.GetArrayLength()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("num_api_keys")
			}
			return err
		}
	}
	protocol.LogWithIndentation(logger, indentation, "- .num_api_keys (%d)", numApiKeys)

	if numApiKeys < 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("Count of ApiKeys cannot be negative: %d", numApiKeys))
	}

	r.ApiKeys = make([]ApiKeyEntry, numApiKeys)
	for i := 0; i < numApiKeys; i++ {
		var apiKeyEntry ApiKeyEntry
		protocol.LogWithIndentation(logger, indentation, "- .ApiKeys[%d]", i)
		if err = apiKeyEntry.Decode(pd, r.Version, logger, indentation+1); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("ApiKeyEntry[%d]", i))
			}
			return err
		}
		r.ApiKeys[i] = apiKeyEntry
	}

	if r.Version >= 1 {
		if r.ThrottleTimeMs, err = pd.GetInt32(); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("throttle_time_ms")
			}
			return err
		}
		protocol.LogWithIndentation(logger, indentation, "- .throttle_time_ms (%d)", r.ThrottleTimeMs)
	}

	if r.Version >= 3 {
		if _, err = pd.GetEmptyTaggedFieldArray(); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("TAG_BUFFER")
			}
			return err
		}
		protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")
	}

	// Check if there are any remaining bytes in the decoder
	if pd.Remaining() != 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("unexpected %d bytes remaining in decoder after decoding ApiVersionsResponseBody", pd.Remaining()))
	}

	return nil
}

type ApiVersionsResponse struct {
	Header headers.ResponseHeader
	Body   ApiVersionsResponseBody
}

func (r *ApiVersionsResponse) Decode(response []byte, logger *logger.Logger) error {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

	logger.Debugf("- .response_header")
	if err := r.Header.Decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("ApiVersions v4")
			return decoder.FormatDetailedError(detailedError.Error())
		}
		return err
	}

	if r.Body.Version == 0 {
		panic("CodeCrafters Internal Error: ApiVersionsResponseBody.Version is not initialized")
	}

	logger.Debugf("- .ResponseBody")
	if err := r.Body.Decode(&decoder, r.Body.Version, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("ApiVersions v4")
			return decoder.FormatDetailedError(detailedError.Error())
		}
		return err
	}

	return nil
}

/********************** Updated Interface *****************/

// ApiKeyEntry__Updated contains the APIs supported by the broker.
type ApiKeyEntry__Updated struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ApiKey contains the API index.
	ApiKey int16
	// MinVersion contains the minimum supported version, inclusive.
	MinVersion int16
	// MaxVersion contains the maximum supported version, inclusive.
	MaxVersion int16
}

func (a *ApiKeyEntry__Updated) Decode(d *decoder.Decoder, version int16, index int) (err error) {
	d.BeginSubSection(fmt.Sprintf("ApiKeys[%d]", index))
	defer d.EndCurrentSubSection()

	a.Version = version
	if a.ApiKey, err = d.GetInt16_Updated("api_key"); err != nil {
		return err
	}

	if a.MinVersion, err = d.GetInt16_Updated("min_version"); err != nil {
		return err
	}

	if a.MaxVersion, err = d.GetInt16_Updated("max_version"); err != nil {
		return err
	}

	if version >= 3 {
		if err := d.ConsumeTagBuffer(); err != nil {
			return err
		}
	}

	return nil
}

type ApiVersionsResponseBody__Updated struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ErrorCode contains the top-level error code.
	ErrorCode int16
	// ApiKeys contains the APIs supported by the broker.
	ApiKeys []ApiKeyEntry__Updated
	// ThrottleTimeMs contains the duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32
}

func (r *ApiVersionsResponseBody__Updated) Decode(d *decoder.Decoder, version int16, logger *logger.Logger, indentation int) (err error) {
	d.BeginSubSection("response_body")
	defer d.EndCurrentSubSection()
	r.Version = version

	if r.ErrorCode, err = d.GetInt16_Updated("error_code"); err != nil {
		return err
	}

	var numApiKeys int

	if r.Version >= 3 {
		numApiKeys, err = d.GetCompactArrayLength_Updated("num_api_keys")

		if err != nil {
			return err
		}
	} else {
		numApiKeys, err = d.GetArrayLength_Updated("num_api_keys")

		if err != nil {
			return err
		}
	}

	if numApiKeys < 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("Count of ApiKeys cannot be negative: %d", numApiKeys))
	}

	r.ApiKeys = make([]ApiKeyEntry__Updated, numApiKeys)

	for i := 0; i < numApiKeys; i++ {
		var apiKeyEntry ApiKeyEntry__Updated

		if err = apiKeyEntry.Decode(d, r.Version, i); err != nil {
			return err
		}

		r.ApiKeys[i] = apiKeyEntry
	}

	if r.Version >= 1 {
		if r.ThrottleTimeMs, err = d.GetInt32_Updated("throttle_time_ms"); err != nil {
			return err
		}
	}

	if r.Version >= 3 {
		if err = d.ConsumeTagBuffer(); err != nil {
			return err
		}
	}

	// Check if there are any remaining bytes in the decoder
	if d.Remaining() != 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("unexpected %d bytes remaining in decoder after decoding ApiVersionsResponseBody", d.Remaining()))
	}

	return nil
}

type ApiVersionsResponse__Updated struct {
	Header headers.ResponseHeader__Updated
	Body   ApiVersionsResponseBody__Updated
}

func (r *ApiVersionsResponse__Updated) Decode(response []byte, logger *logger.Logger) error {
	logger.UpdateLastSecondaryPrefix("Decoder")
	decoder := decoder.Decoder{}
	decoder.InitNew(response, logger)
	defer logger.ResetSecondaryPrefixes()

	if err := r.Header.Decode(&decoder); err != nil {
		return err
	}

	if r.Body.Version == 0 {
		panic("CodeCrafters Internal Error: ApiVersionsResponseBody.Version is not initialized")
	}

	if err := r.Body.Decode(&decoder, r.Body.Version, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("ApiVersions v4")
			return decoder.FormatDetailedError(detailedError.Error())
		}
		return err
	}

	return nil
}
