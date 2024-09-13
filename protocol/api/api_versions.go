package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

func GetAPIVersions(prettyPrint bool) {
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.Connect(); err != nil {
		panic(err)
	}
	defer broker.Close()

	response, err := ApiVersions(broker, &ApiVersionsRequestBody{Version: 4, ClientSoftwareName: "kafka-cli", ClientSoftwareVersion: "0.1"})
	if err != nil {
		panic(err)
	}

	if prettyPrint {
		PrintAPIVersions(response)
	}
}

func EncodeApiVersionsRequest(request *ApiVersionsRequest) []byte {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	request.Header.EncodeV1(&encoder)
	request.Body.Encode(&encoder)
	messageBytes := encoder.PackMessage()

	return messageBytes
}

func DecodeApiVersionsHeader(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, error) {
	decoder := decoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	// APIVersions always uses Header v0
	if err := responseHeader.DecodeV0(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("Response Header").WithAddedContext("ApiVersions v3")
		}
		return nil, err
	}

	return &responseHeader, nil
}

// DecodeApiVersionsHeaderAndResponse decodes the header and response
// If an error is encountered while decoding, the returned objects are nil
func DecodeApiVersionsHeaderAndResponse(response []byte, version int16, logger *logger.Logger) (*ResponseHeader, *ApiVersionsResponse, error) {
	decoder := decoder.RealDecoder{}
	decoder.Init(response)
	logger.UpdateSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefix()

	responseHeader := ResponseHeader{}
	logger.Debugf("- .ResponseHeader")
	if err := responseHeader.DecodeV0(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("ApiVersions v3")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	apiVersionsResponse := ApiVersionsResponse{Version: version}
	logger.Debugf("- .ResponseBody")
	if err := apiVersionsResponse.Decode(&decoder, version, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("ApiVersions v3")
			return nil, nil, decoder.FormatDetailedError(detailedError.Error())
		}
		return nil, nil, err
	}

	return &responseHeader, &apiVersionsResponse, nil
}

// ApiVersions returns api version response or error
func ApiVersions(b *protocol.Broker, requestBody *ApiVersionsRequestBody) (*ApiVersionsResponse, error) {
	header := RequestHeader{
		ApiKey:        18,
		ApiVersion:    requestBody.Version,
		CorrelationId: 0,
		ClientId:      requestBody.ClientSoftwareName,
	}
	request := ApiVersionsRequest{
		Header: header,
		Body:   *requestBody,
	}
	message := EncodeApiVersionsRequest(&request)

	response, err := b.SendAndReceive(message)
	if err != nil {
		return nil, err
	}

	_, apiVersionsResponse, err := DecodeApiVersionsHeaderAndResponse(response, requestBody.Version, logger.GetLogger(true, ""))
	if err != nil {
		return nil, err
	}

	return apiVersionsResponse, nil
}

func PrintAPIVersions(response *ApiVersionsResponse) {
	fmt.Printf("API versions supported by the broker are:\n")
	fmt.Println("API Key\tMinVersion\tMaxVersion\t")
	apiVersionKeys := response.ApiKeys
	// For each API, the broker will return the minimum and maximum supported version
	for _, key := range apiVersionKeys {
		fmt.Println(key.ApiKey, "\t", key.MinVersion, "\t", key.MaxVersion)
	}
}
