package utils

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	kafka_interface "github.com/codecrafters-io/kafka-tester/protocol/interface"
)

func GetEncodedBytes(encodableObject kafka_interface.Encodable) []byte {
	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 1024))
	encodableObject.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	return encodedBytes
}

func GetEncodedLength(encodableObject kafka_interface.Encodable) int {
	return len(GetEncodedBytes(encodableObject))
}

func APIKeyToName(apiKey int16) string {
	switch apiKey {
	case 0:
		return "Produce"
	case 1:
		return "Fetch"
	case 18:
		return "ApiVersions"
	case 19:
		return "CreateTopics"
	case 75:
		return "DescribeTopicPartitions"
	default:
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unknown API key: %v", apiKey))
	}
}

func GetFormattedHexdump(data []byte) string {
	// This is used for logs
	// Contains headers + vertical & horizontal separators + offset
	// We use a different format for the error logs
	var formattedHexdump strings.Builder
	var asciiChars strings.Builder

	formattedHexdump.WriteString("Idx  | Hex                                             | ASCII\n")
	formattedHexdump.WriteString("-----+-------------------------------------------------+-----------------\n")

	for i, b := range data {
		if i%16 == 0 && i != 0 {
			formattedHexdump.WriteString("| " + asciiChars.String() + "\n")
			asciiChars.Reset()
		}
		if i%16 == 0 {
			formattedHexdump.WriteString(fmt.Sprintf("%04x | ", i))
		}
		formattedHexdump.WriteString(fmt.Sprintf("%02x ", b))

		// Add ASCII representation
		if b >= 32 && b <= 126 {
			asciiChars.WriteByte(b)
		} else {
			asciiChars.WriteByte('.')
		}
	}

	// Pad the last line if necessary
	if len(data)%16 != 0 {
		padding := 16 - (len(data) % 16)
		for i := 0; i < padding; i++ {
			formattedHexdump.WriteString("   ")
		}
	}

	// Add the final ASCII representation
	formattedHexdump.WriteString("| " + asciiChars.String())

	return formattedHexdump.String()
}
