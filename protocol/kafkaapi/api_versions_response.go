package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion"
	wireDecoder "github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/instrumented_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ApiVersionsResponse struct {
	Header headers.ResponseHeader
	Body   ApiVersionsResponseBody
}

func DecodeApiVersionsResponse(responseBytes []byte, logger *logger.Logger, valueAssertions value_assertion.ValueAssertionCollection) (ApiVersionsResponse, error) {
	response := ApiVersionsResponse{
		Header: headers.ResponseHeader{Version: 0},
		Body:   ApiVersionsResponseBody{Version: 4},
	}

	decoder := instrumented_decoder.NewInstrumentedDecoder(responseBytes, logger, valueAssertions)

	decoder.BeginSubSection("ApiVersionsResponse")
	defer decoder.EndCurrentSubSection()

	// TODO[PaulRefactor]: This pattern of Header.Decode, Body.Decoder seems like it'll be common among all response. See if we can extract?
	if err := response.Header.Decode(decoder); err != nil {
		return response, err
	}

	if err := response.Body.Decode(decoder); err != nil {
		return response, err
	}

	return response, nil
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

func (r *ApiVersionsResponseBody) Decode(decoder *instrumented_decoder.InstrumentedDecoder) (err error) {
	if r.Version == 0 {
		panic("CodeCrafters Internal Error: ApiVersionsResponseBody.Version is not initialized")
	} else if r.Version < 3 {
		return fmt.Errorf("unsupported ApiVersionsResponseBody version: %d. Expected version: >= 3", r.Version)
	}

	decoder.BeginSubSection("ApiVersionsResponseBody")
	defer decoder.EndCurrentSubSection()

	if r.ErrorCode, err = decoder.ReadInt16("ErrorCode"); err != nil {
		return err
	}

	// DecodeApiKeysEntry
	if r.ApiKeys, err = wireDecoder.ReadCompactArray[ApiKeyEntry](decoder.Decoder, "ApiKeys"); err != nil {
		return err
	}

	if r.ThrottleTimeMs, err = decoder.ReadInt32("ThrottleTimeMs"); err != nil {
		return err
	}

	if err = decoder.ConsumeTagBuffer(); err != nil {
		return err
	}

	// Check if there are any remaining bytes in the decoder
	if decoder.RemainingBytesCount() != 0 {
		return fmt.Errorf("unexpected %d bytes remaining in decoder after decoding ApiVersionsResponseBody", decoder.RemainingBytesCount())
	}

	return nil
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

func (a *ApiKeyEntry) Decode(decoder *wireDecoder.Decoder, variableName string) (err error) {
	decoderCallbacks := decoder.GetCallBacks()
	decoderCallbacks.OnCompositeDecodingStart(variableName)
	defer decoderCallbacks.OnCompositeDecodingEnd()

	if a.ApiKey, err = decoder.ReadInt16("APIKey"); err != nil {
		return err
	}

	if a.MinVersion, err = decoder.ReadInt16("MinVersion"); err != nil {
		return err
	}

	if a.MaxVersion, err = decoder.ReadInt16("MaxVersion"); err != nil {
		return err
	}

	if err = decoder.ConsumeTagBuffer(); err != nil {
		return err
	}

	return nil
}
