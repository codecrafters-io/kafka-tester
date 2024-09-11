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
func (s InspectableHexDump) FormatWithHighlightedOffset(highlightOffset int) string {
	s = s.TruncateAroundOffset(highlightOffset)

	lines := []string{}

	byteRangeStart, byteRangeEnd := s.GetByteIndicesAfterTruncation(highlightOffset)
	lines = append(lines, s.FormmattedStringWithHeading(byteRangeStart, byteRangeEnd))

	offsetPointerLine1 := ""
	offsetPointerLine1 += strings.Repeat(" ", s.GetOffsetInHexdump(highlightOffset)) + "^"

	diff := s.GetOffsetInAsciiString(highlightOffset) - len(offsetPointerLine1)
	offsetPointerLine1 += strings.Repeat(" ", diff) + "^"

	lines = append(lines, offsetPointerLine1)
	return strings.Join(lines, "\n")
}

func (s InspectableHexDump) FormattedString() string {
	return protocol.GetFormattedHexdumpForErrors(s.bytes)
}

func (s InspectableHexDump) FormmattedStringWithHeading(byteRangeStart int, byteRangeEnd int) string {
	heading := fmt.Sprintf("Hex (bytes %d-%d)", byteRangeStart, byteRangeEnd)
	prefixLength := 50 - 2
	heading += strings.Repeat(" ", prefixLength-len(heading)) + "| ASCII\n"
	return heading + s.FormattedString()
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

func (s InspectableHexDump) GetByteIndicesAfterTruncation(offset int) (int, int) {
	start := max(0, offset-5)
	end := max(0, min(len(s.bytes), start+16))
	return start, end
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

	formattedBytesBefore := string(s.bytes[:byteOffset])
	hexdumpLength := len(formattedBytesBefore) * 3
	if hexdumpLength == 0 {
		return 1
	}

	return hexdumpLength + 1
}

func (s InspectableHexDump) GetOffsetInAsciiString(byteOffset int) int {
	if s.truncationStartIndex != 0 {
		byteOffset = byteOffset - s.truncationStartIndex
	}
	prefixLength := 50

	formattedBytesBefore := string(s.bytes[:byteOffset])
	asciiLength := len(formattedBytesBefore)
	if asciiLength == 0 {
		return prefixLength
	}

	return asciiLength + prefixLength
}
