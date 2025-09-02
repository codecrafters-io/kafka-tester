package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ApiVersionsResponse struct {
	Header headers.ResponseHeader
	Body   ApiVersionsResponseBody
}

func (r *ApiVersionsResponse) Decode(response []byte, logger *logger.Logger) error {
	decoder := decoder.NewDecoder(response, logger)

	if err := r.Header.Decode(decoder); err != nil {
		return err
	}

	if err := r.Body.Decode(decoder); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.AddContexts("ApiVersions Response")
			return decoder.FormatDetailedError(detailedError.Error())
		}

		return err
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

func (r *ApiVersionsResponseBody) Decode(d *decoder.Decoder) (err error) {
	if r.Version == 0 {
		panic("CodeCrafters Internal Error: ApiVersionsResponseBody.Version is not initialized")
	}

	d.BeginSubSection("response_body")
	defer d.EndCurrentSubSection()

	if r.ErrorCode, err = d.ReadInt16("error_code"); err != nil {
		return err
	}

	var numApiKeys int

	if r.Version >= 3 {
		numApiKeys, err = d.ReadCompactArrayLength("ApiKeys.Length")

		if err != nil {
			return err
		}
	} else {
		numApiKeys, err = d.ReadArrayLength("ApiKeys.Length")

		if err != nil {
			return err
		}
	}

	if numApiKeys < 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("Count of ApiKeys cannot be negative: %d", numApiKeys))
	}

	r.ApiKeys = make([]ApiKeyEntry, numApiKeys)

	for i := 0; i < numApiKeys; i++ {
		var apiKeyEntry ApiKeyEntry
		apiKeyEntryName := fmt.Sprintf("ApiKeys[%d]", i)

		if err = apiKeyEntry.Decode(d, r.Version, apiKeyEntryName); err != nil {
			return err
		}

		r.ApiKeys[i] = apiKeyEntry
	}

	if r.Version >= 1 {
		if r.ThrottleTimeMs, err = d.ReadInt32("throttle_time_ms"); err != nil {
			return err
		}
	}

	if r.Version >= 3 {
		if err = d.ConsumeTagBuffer(); err != nil {
			return err
		}
	}

	// Check if there are any remaining bytes in the decoder
	if d.UnreadBytesCount() != 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("unexpected %d bytes remaining in decoder after decoding ApiVersionsResponseBody", d.UnreadBytesCount()))
	}

	return nil
}

// ApiKeyEntry contains the APIs supported by the broker.
type ApiKeyEntry struct {
	// ApiKey contains the API index.
	ApiKey int16
	// MinVersion contains the minimum supported version, inclusive.
	MinVersion int16
	// MaxVersion contains the maximum supported version, inclusive.
	MaxVersion int16
}

func (a *ApiKeyEntry) Decode(d *decoder.Decoder, version int16, variableName string) (err error) {
	d.BeginSubSection(variableName)
	defer d.EndCurrentSubSection()

	if a.ApiKey, err = d.ReadInt16("api_key"); err != nil {
		return err
	}

	if a.MinVersion, err = d.ReadInt16("min_version"); err != nil {
		return err
	}

	if a.MaxVersion, err = d.ReadInt16("max_version"); err != nil {
		return err
	}

	if version >= 3 {
		if err := d.ConsumeTagBuffer(); err != nil {
			return err
		}
	}

	return nil
}
