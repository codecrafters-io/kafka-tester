package decoder

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol"
)

type InspectableHexDump struct {
	bytes []byte

	truncationStartIndex int
}

func (s InspectableHexDump) String() string {
	return string(s.bytes)
}

func NewInspectableHexDump(bytes []byte) InspectableHexDump {
	return InspectableHexDump{bytes: bytes}
}

// FormatWithHighlightedOffset returns a string that represents the hexdump with the byteOffset highlighted
//
// For example, if called with highlightOffset 4, highlightText "error" and formattedString "Received: ", the return value will be:
//
// > Received: "+OK\r\n"
// >                 ^ error
func (s InspectableHexDump) FormatWithHighlightedOffset(highlightOffset int, highlightText string, formattedStringPrefix string, formattedStringSuffix string) string {
	s = s.TruncateAroundOffset(highlightOffset)

	lines := []string{}

	lines = append(lines, fmt.Sprintf("%s%s%s", formattedStringPrefix, s.FormattedString(), formattedStringSuffix))

	offsetPointerLine1 := ""
	offsetPointerLine1 += strings.Repeat(" ", len(formattedStringPrefix)+s.GetOffsetInHexdump(highlightOffset))
	offsetPointerLine1 += "^ " + highlightText

	offsetPointerLine2 := ""
	offsetPointerLine2 += strings.Repeat(" ", len(formattedStringPrefix)+s.GetOffsetInAsciiString(highlightOffset))
	offsetPointerLine2 += "^ " + highlightText

	lines = append(lines, offsetPointerLine1)
	lines = append(lines, offsetPointerLine2)
	return strings.Join(lines, "\n")
}

func (s InspectableHexDump) FormattedString() string {
	return protocol.GetFormattedHexdump(s.bytes)
}

func (s InspectableHexDump) TruncateAroundOffset(offset int) InspectableHexDump {
	// We've got about 16 raw bytes to use in the terminal line.
	start := max(0, offset-5)
	end := max(0, min(len(s.bytes), start+16))

	return InspectableHexDump{
		bytes:                s.bytes[start:end],
		truncationStartIndex: start,
	}
}

// GetOffsetInFormattedString returns a string that represents the byteOffset in the formatted string
//
// For example:
//   - If the string is "+OK\r\n"
//   - And byteOffset is 4 (i.e. \n, the 5th byte)
//   - The return value will be 6 (i.e. the 6th character in the formatted string)
func (s InspectableHexDump) GetOffsetInHexdump(byteOffset int) int {
	if s.truncationStartIndex != 0 {
		byteOffset = byteOffset - s.truncationStartIndex
	}
	prefixLength := 5

	formattedBytesBefore := string(s.bytes[:byteOffset])
	hexdumpLength := len(formattedBytesBefore) * 3
	if hexdumpLength == 0 {
		return prefixLength
	}

	return hexdumpLength + prefixLength - 1
}

func (s InspectableHexDump) GetOffsetInAsciiString(byteOffset int) int {
	if s.truncationStartIndex != 0 {
		byteOffset = byteOffset - s.truncationStartIndex
	}
	prefixLength := 56

	formattedBytesBefore := string(s.bytes[:byteOffset])
	asciiLength := len(formattedBytesBefore)
	if asciiLength == 0 {
		return prefixLength
	}

	return asciiLength + prefixLength - 1
}
