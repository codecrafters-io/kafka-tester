package protocol

import (
	"fmt"
	"sort"
	"strings"

	"github.com/codecrafters-io/tester-utils/logger"
)

func PrintHexdump(data []byte) {
	fmt.Printf("Hexdump of data:\n")
	for i, b := range data {
		if i%16 == 0 {
			fmt.Printf("\n%04x  ", i)
		}
		fmt.Printf("%02x ", b)
	}
	fmt.Println()
}

func GetFormattedHexdumpForErrors(data []byte) string {
	var formattedHexdump strings.Builder
	var asciiChars strings.Builder

	for i, b := range data {
		if i%16 == 0 && i != 0 {
			formattedHexdump.WriteString("| " + asciiChars.String() + "\n")
			asciiChars.Reset()
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

func LogWithIndentation(logger *logger.Logger, indentation int, message string, args ...interface{}) {
	logger.Debugf(fmt.Sprintf("%s%s", strings.Repeat(" ", indentation*2), message), args...)
}

func GetSortedValues[T string](values []T) []T {
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	return values
}
