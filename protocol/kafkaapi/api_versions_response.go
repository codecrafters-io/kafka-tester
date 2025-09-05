package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/kafka-tester/protocol/value_storing_decoder"
)

type ApiVersionsResponse struct {
	Header headers.ResponseHeader
	Body   ApiVersionsResponseBody
}

func DecodeApiVersionsResponse(decoder *value_storing_decoder.ValueStoringDecoder) (ApiVersionsResponse, error) {
	response := ApiVersionsResponse{
		Header: headers.ResponseHeader{Version: 0},
		Body:   ApiVersionsResponseBody{Version: 4},
	}

	decoder.PushLocatorSegment("ApiVersionsResponse")
	defer decoder.PopLocatorSegment()

	// TODO[PaulRefactor]: This pattern of Header.Decode, Body.Decoder seems like it'll be common among all response. See if we can extract?
	if err := response.Header.Decode(decoder); err != nil {
		return response, err
	}

	if err := response.Body.Decode(decoder); err != nil {
		return response, err
	}

	// Check if there are any remaining bytes in the decoder
	if decoder.RemainingBytesCount() != 0 {
		return response, fmt.Errorf("unexpected %d bytes remaining in decoder after decoding ApiVersionsResponseBody", decoder.RemainingBytesCount())
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

func (r *ApiVersionsResponseBody) Decode(decoder *value_storing_decoder.ValueStoringDecoder) (err error) {
	if r.Version == 0 {
		panic("CodeCrafters Internal Error: ApiVersionsResponseBody.Version is not initialized")
	} else if r.Version < 3 {
		return fmt.Errorf("unsupported ApiVersionsResponseBody version: %d. Expected version: >= 3", r.Version)
	}

	decoder.PushLocatorSegment("ApiVersionsResponseBody")
	defer decoder.PopLocatorSegment()

	if r.ErrorCode, err = decoder.ReadInt16("ErrorCode"); err != nil {
		return err
	}

	// DecodeApiKeysEntry
	if r.ApiKeys, err = r.decodeApiKeyEntries(decoder); err != nil {
		return err
	}

	if r.ThrottleTimeMs, err = decoder.ReadInt32("ThrottleTimeMs"); err != nil {
		return err
	}

	if err = decoder.ConsumeTagBuffer(); err != nil {
		return err
	}

	return nil
}

func (r *ApiVersionsResponseBody) decodeApiKeyEntries(decoder *value_storing_decoder.ValueStoringDecoder) ([]ApiKeyEntry, error) {
	lengthValue, err := decoder.ReadCompactArrayLength("ApiKeys.Length")
	if err != nil {
		return nil, err
	}

	apiKeyEntries := make([]ApiKeyEntry, lengthValue.ActualLength())
	for i := 0; i < int(lengthValue.ActualLength()); i++ {
		apiKeyEntryLocator := fmt.Sprintf("ApiKeys[%d]", i)
		apiKeyEntry := ApiKeyEntry{}

		if err := apiKeyEntry.Decode(decoder, apiKeyEntryLocator); err != nil {
			return nil, err
		}

		apiKeyEntries[i] = apiKeyEntry
	}

	return apiKeyEntries, nil
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

func (a *ApiKeyEntry) Decode(decoder *value_storing_decoder.ValueStoringDecoder, locator string) (err error) {
	decoder.PushLocatorSegment(locator)

	// Ensure the locator segment remains if there's an error (used in error messages)
	// TODO[PaulRefactor]: See if we can bake error context in?
	defer func() {
		if err == nil {
			decoder.PopLocatorSegment()
		}
	}()

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
