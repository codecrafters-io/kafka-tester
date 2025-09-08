package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_decoder_2"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ApiVersionsResponse struct {
	Header headers.ResponseHeader
	Body   ApiVersionsResponseBody
}

func (r *ApiVersionsResponse) Decode(response []byte, logger *logger.Logger) (err error) {
	decoder := decoder.NewDecoder(response, logger)

	decoder.BeginSubSection("ApiVersionsResponse")
	defer decoder.EndCurrentSubSection()

	defer func() {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.AddContexts("ApiVersionsResponse")
			err = decoder.FormatDetailedError(detailedError.Error())
		}
	}()

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

func (r *ApiVersionsResponseBody) Decode(d *decoder.Decoder) (err error) {
	if r.Version == 0 {
		panic("CodeCrafters Internal Error: ApiVersionsResponseBody.Version is not initialized")
	} else if r.Version < 3 {
		return fmt.Errorf("unsupported ApiVersionsResponseBody version: %d. Expected version: >= 3", r.Version)
	}

	d.BeginSubSection("ApiVersionsResponseBody")
	defer d.EndCurrentSubSection()

	defer func() {
		if decodingError, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingError.AddContexts("ApiVersionsResponseBody")
		}
	}()

	if r.ErrorCode, err = d.ReadInt16("ErrorCode"); err != nil {
		return err
	}

	numApiKeys, err := d.ReadCompactArrayLength("ApiKeys.Length")
	if err != nil {
		return err
	}

	r.ApiKeys = make([]ApiKeyEntry, numApiKeys.ActualLength())

	for i := 0; i < len(r.ApiKeys); i++ {
		var apiKeyEntry ApiKeyEntry
		apiKeyEntryName := fmt.Sprintf("ApiKeys[%d]", i)

		if err = apiKeyEntry.Decode(d, apiKeyEntryName); err != nil {
			return err
		}

		r.ApiKeys[i] = apiKeyEntry
	}

	if r.ThrottleTimeMs, err = d.ReadInt32("ThrottleTimeMs"); err != nil {
		return err
	}

	if err = d.ConsumeTagBuffer(); err != nil {
		return err
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
	ApiKey value.Int16
	// MinVersion contains the minimum supported version, inclusive.
	MinVersion value.Int16
	// MaxVersion contains the maximum supported version, inclusive.
	MaxVersion value.Int16
}

func (a *ApiKeyEntry) Decode(d *decoder.Decoder, variableName string) (err error) {
	d.BeginSubSection(variableName)
	defer d.EndCurrentSubSection()

	defer func() {
		if decodingError, ok := err.(*errors.PacketDecodingError); ok {
			err = decodingError.AddContexts(variableName)
		}
	}()

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
