package utils

import (
	"fmt"
	"strings"
)

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

func ErrorCodeToName(errorCode int16) string {
	errorCodes := map[int16]string{
		0:   "NO_ERROR",
		3:   "UNKNOWN_TOPIC_OR_PARTITION",
		35:  "UNSUPPORTED_VERSION",
		100: "UNKNOWN_TOPIC_ID",
	}

	errorCodeName, ok := errorCodes[errorCode]
	if !ok {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Expected %d to be in errorCodes map", errorCode))
	}

	return errorCodeName
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
