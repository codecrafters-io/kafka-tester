package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	wireDecoder "github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ApiVersionsResponse struct {
	Header headers.ResponseHeader
	Body   ApiVersionsResponseBody
}

func (r *ApiVersionsResponse) Decode(responseBytes []byte, logger *logger.Logger, assertion assertions.Assertion) (err error) {
	decoder := wireDecoder.NewInstrumentedDecoder(responseBytes, logger, assertion)

	decoder.BeginSubSection("ApiVersionsResponse")
	defer decoder.EndCurrentSubSection()

	if err = r.Header.Decode(decoder); err != nil {
		return err
	}

	if err = r.Body.Decode(decoder); err != nil {
		return err
	}

	return nil
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

func (r *ApiVersionsResponseBody) Decode(d *wireDecoder.Decoder) (err error) {
	if r.Version == 0 {
		panic("CodeCrafters Internal Error: ApiVersionsResponseBody.Version is not initialized")
	} else if r.Version < 3 {
		return fmt.Errorf("unsupported ApiVersionsResponseBody version: %d. Expected version: >= 3", r.Version)
	}

	d.BeginSubSection("ApiVersionsResponseBody")
	defer d.EndCurrentSubSection()

	if r.ErrorCode, err = d.ReadInt16("ErrorCode"); err != nil {
		return err
	}

	// DecodeApiKeysEntry
	if r.ApiKeys, err = wireDecoder.ReadCompactArray[ApiKeyEntry](d, "ApiKeys"); err != nil {
		return err
	}

	if r.ThrottleTimeMs, err = d.ReadInt32("ThrottleTimeMs"); err != nil {
		return err
	}

	if err = d.ConsumeTagBuffer(); err != nil {
		return err
	}

	// Check if there are any remaining bytes in the decoder
	if d.RemainingBytesCount() != 0 {
		return fmt.Errorf("unexpected %d bytes remaining in decoder after decoding ApiVersionsResponseBody", d.RemainingBytesCount())
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

func (a ApiKeyEntry) Decode(d *wireDecoder.Decoder, variableName string) (err error) {
	d.BeginSubSection(variableName)
	defer d.EndCurrentSubSection()

	if a.ApiKey, err = d.ReadInt16("APIKey"); err != nil {
		return err
	}

	if a.MinVersion, err = d.ReadInt16("MinVersion"); err != nil {
		return err
	}

	if a.MaxVersion, err = d.ReadInt16("MaxVersion"); err != nil {
		return err
	}

	if err = d.ConsumeTagBuffer(); err != nil {
		return err
	}

	return nil
}
