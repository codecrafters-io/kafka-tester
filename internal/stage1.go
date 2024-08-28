package internal

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

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

func (a *ApiVersionsResponseKey) decode(pd *decoder.RealDecoder, version int16) (err error) {
	a.Version = version
	if a.ApiKey, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode: %w", err)
	}

	if a.MinVersion, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode: %w", err)
	}

	if a.MaxVersion, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode: %w", err)
	}

	if version >= 3 {
		if _, err := pd.GetEmptyTaggedFieldArray(); err != nil {
			return fmt.Errorf("failed to decode: %w", err)
		}
	}

	return nil
}

func getAPIVersionsTriggerErrors(broker net.Conn) (*ApiVersionsResponse, error) {
	// request := kafkaapi.ApiVersionsRequest{Version: 2, ClientSoftwareName: "kafka-cli", ClientSoftwareVersion: "0.1"}

	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 100))

	request_api_key := int16(20000)
	request_api_version := int16(2)
	correlation_id := int32(0)
	client_id := ""

	encoder.PutInt16(request_api_key)
	encoder.PutInt16(request_api_version)
	encoder.PutInt32(correlation_id)
	encoder.PutString(client_id)

	encoded := encoder.Bytes()
	encoded = encoded[:encoder.Offset()]
	req_length := int32(len(encoded))
	message := make([]byte, 4+len(encoded))
	binary.BigEndian.PutUint32(message[:4], uint32(req_length))
	copy(message[4:], encoded[:len(message)])
	// message[20] = 0x00
	fmt.Printf("Message: %x\n", message)
	fmt.Println(len(message))

	broker.Write(message)
	// read response from broker
	response := make([]byte, 4) // length
	broker.Read(response)
	length := int32(binary.BigEndian.Uint32(response[0:]))
	fmt.Printf("Length: %d\n", length)

	response = make([]byte, 1024)
	broker.Read(response)
	// fmt.Printf("Hexdump of response:\n")
	// for i, b := range response {
	// 	if i%16 == 0 {
	// 		fmt.Printf("\n%04x  ", i)
	// 	}
	// 	fmt.Printf("%02x ", b)
	// }
	// fmt.Println()

	// response_header.go
	decoder := decoder.RealDecoder{}
	decoder.Init(response)

	response_correlation_id, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Correlation ID: %d\n", response_correlation_id)
	// api_versions_response.go
	error_code, err := decoder.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Error code: %d\n", error_code)
	api_keys_length, err := decoder.GetArrayLength()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("API keys length: %d\n", api_keys_length)
	api_keys := make([]ApiVersionsResponseKey, api_keys_length)
	for i := 0; i < api_keys_length; i++ {
		var block ApiVersionsResponseKey
		if err = block.decode(&decoder, request_api_version); err != nil {
			return nil, fmt.Errorf("failed to decode block: %w", err)
		}
		api_keys[i] = block
	}

	for _, key := range api_keys {
		fmt.Printf("API Key: %d, MinVersion: %d, MaxVersion: %d\n", key.ApiKey, key.MinVersion, key.MaxVersion)
	}

	// return response, nil
	return nil, nil
}

func getAPIVersionsV2(broker net.Conn) (*ApiVersionsResponse, error) {
	// request := kafkaapi.ApiVersionsRequest{Version: 2, ClientSoftwareName: "kafka-cli", ClientSoftwareVersion: "0.1"}

	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 100))

	request_api_key := int16(18)
	request_api_version := int16(2)
	correlation_id := int32(0)
	client_id := "kafka-cli"

	encoder.PutInt16(request_api_key)
	encoder.PutInt16(request_api_version)
	encoder.PutInt32(correlation_id)
	encoder.PutString(client_id)

	encoded := encoder.Bytes()
	encoded = encoded[:encoder.Offset()]
	length := int32(len(encoded))
	message := make([]byte, 4+len(encoded))
	binary.BigEndian.PutUint32(message[:4], uint32(length))
	copy(message[4:], encoded)

	broker.Write(message)
	// read response from broker
	response := make([]byte, 4) // length
	broker.Read(response)
	length = int32(binary.BigEndian.Uint32(response))

	response = make([]byte, length)
	broker.Read(response)
	fmt.Printf("Hexdump of response:\n")
	for i, b := range response {
		if i%16 == 0 {
			fmt.Printf("\n%04x  ", i)
		}
		fmt.Printf("%02x ", b)
	}
	fmt.Println()

	// response_header.go
	decoder := decoder.RealDecoder{}
	decoder.Init(response)

	response_correlation_id, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Correlation ID: %d\n", response_correlation_id)
	// api_versions_response.go
	error_code, err := decoder.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Error code: %d\n", error_code)
	api_keys_length, err := decoder.GetArrayLength()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("API keys length: %d\n", api_keys_length)
	api_keys := make([]ApiVersionsResponseKey, api_keys_length)
	for i := 0; i < api_keys_length; i++ {
		var block ApiVersionsResponseKey
		fmt.Printf("Decoding block %d\n", i)
		if err = block.decode(&decoder, request_api_version); err != nil {
			return nil, fmt.Errorf("failed to decode block: %w", err)
		}
		api_keys[i] = block
	}

	throttle_time, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Throttle time: %d\n", throttle_time)

	if decoder.Remaining() != 0 {
		return nil, fmt.Errorf("remaining bytes in decoder: %d", decoder.Remaining())
	}

	for _, key := range api_keys {
		fmt.Printf("API Key: %d, MinVersion: %d, MaxVersion: %d\n", key.ApiKey, key.MinVersion, key.MaxVersion)
	}

	// return response, nil
	return nil, nil
}

func getAPIVersionsV3(broker net.Conn) (*ApiVersionsResponse, error) {
	// request := kafkaapi.ApiVersionsRequest{Version: 2, ClientSoftwareName: "kafka-cli", ClientSoftwareVersion: "0.1"}

	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 100))

	request_api_key := int16(18)
	request_api_version := int16(3)
	correlation_id := int32(0)
	client_id := "console-producer"

	encoder.PutInt16(request_api_key)
	encoder.PutInt16(request_api_version)
	encoder.PutInt32(correlation_id)
	encoder.PutString(client_id)

	if request_api_version >= 3 {
		encoder.PutEmptyTaggedFieldArray()
		client_software_name := "apache-kafka-java"
		client_software_version := "3.8.0"
		encoder.PutCompactString(client_software_name)
		encoder.PutCompactString(client_software_version)
		encoder.PutEmptyTaggedFieldArray()
	}

	encoded := encoder.Bytes()
	encoded = encoded[:encoder.Offset()]
	length := int32(len(encoded))
	message := make([]byte, 4+len(encoded))
	binary.BigEndian.PutUint32(message[:4], uint32(length))
	copy(message[4:], encoded)

	broker.Write(message)
	// read response from broker
	response := make([]byte, 4) // length
	broker.Read(response)
	length = int32(binary.BigEndian.Uint32(response))

	response = make([]byte, length)
	broker.Read(response)
	fmt.Printf("Hexdump of response:\n")
	for i, b := range response {
		if i%16 == 0 {
			fmt.Printf("\n%04x  ", i)
		}
		fmt.Printf("%02x ", b)
	}
	fmt.Println()

	// response_header.go
	decoder := decoder.RealDecoder{}
	decoder.Init(response)

	response_correlation_id, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Correlation ID: %d\n", response_correlation_id)
	// api_versions_response.go
	error_code, err := decoder.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Error code: %d\n", error_code)
	api_keys_length, err := decoder.GetUVarint()
	api_keys_length -= 1
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("API keys length: %d\n", api_keys_length)
	api_keys := make([]ApiVersionsResponseKey, api_keys_length)
	for i := uint64(0); i < api_keys_length; i++ {
		var block ApiVersionsResponseKey
		fmt.Printf("Decoding block %d with version %d\n", i, request_api_version)
		if err = block.decode(&decoder, request_api_version); err != nil {
			return nil, fmt.Errorf("failed to decode block: %w", err)
		}
		api_keys[i] = block
	}

	throttle_time, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Throttle time: %d\n", throttle_time)

	for _, key := range api_keys {
		fmt.Printf("API Key: %d, MinVersion: %d, MaxVersion: %d\n", key.ApiKey, key.MinVersion, key.MaxVersion)
	}

	decoder.GetEmptyTaggedFieldArray()

	if decoder.Remaining() != 0 {
		return nil, fmt.Errorf("remaining bytes in decoder: %d", decoder.Remaining())
	}

	// return response, nil
	return nil, nil
}

func getAPIVersionsCorrelationId(broker net.Conn) (*ApiVersionsResponse, error) {
	// request := kafkaapi.ApiVersionsRequest{Version: 2, ClientSoftwareName: "kafka-cli", ClientSoftwareVersion: "0.1"}

	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 100))

	request_api_key := int16(18)
	request_api_version := int16(2)
	correlation_id := int32(0)
	client_id := "kafka-cli"

	encoder.PutInt16(request_api_key)
	encoder.PutInt16(request_api_version)
	encoder.PutInt32(correlation_id)
	encoder.PutString(client_id)

	encoded := encoder.Bytes()
	encoded = encoded[:encoder.Offset()]
	length := int32(len(encoded))
	message := make([]byte, 4+len(encoded))
	binary.BigEndian.PutUint32(message[:4], uint32(length))
	copy(message[4:], encoded)

	broker.Write(message)
	// read response from broker
	response := make([]byte, 4) // length
	broker.Read(response)
	length = int32(binary.BigEndian.Uint32(response[0:]))

	response = make([]byte, length-4)
	broker.Read(response)
	fmt.Printf("Hexdump of response:\n")
	for i, b := range response {
		if i%16 == 0 {
			fmt.Printf("\n%04x  ", i)
		}
		fmt.Printf("%02x ", b)
	}
	fmt.Println()

	// response_header.go
	decoder := decoder.RealDecoder{}
	decoder.Init(response)

	response_correlation_id, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Correlation ID: %d\n", response_correlation_id)

	return nil, nil
}

// // ApiVersions return api version response or error
// func (b *Broker) ApiVersions(request *ApiVersionsRequest) (*ApiVersionsResponse, error) {
// 	response := new(ApiVersionsResponse)

// 	err := b.sendAndReceive(request, response)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return response, nil
// }

func printAPIVersions(response *ApiVersionsResponse) {
	fmt.Printf("API versions supported by the broker are:\n")
	fmt.Println("API Key\tMinVersion\tMaxVersion\t")
	apiVersionKeys := response.ApiKeys
	// For each API, the broker will return the minimum and maximum supported version
	for _, key := range apiVersionKeys {
		fmt.Println(key.ApiKey, "\t", key.MinVersion, "\t", key.MaxVersion)
	}
}

func connect(address string) (net.Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Failed to connect to broker %s: %s\n", address, err)
		return nil, err
	}
	return conn, nil
}

func GetAPIVersions(prettyPrint bool) {
	broker, err := connect("localhost:9092")
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	response, err := getAPIVersionsV3(broker)
	if err != nil {
		panic(err)
	}

	if prettyPrint {
		printAPIVersions(response)
	}
}
