package inspectable_hex_dump

import (
	"fmt"
	"strings"
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

// FormatWithHighlightedOffsets returns a string that represents the hexdump with the byteOffset highlighted
//
// For example, if called with startOffset 4 and endOffset 6, the return value will be something like this:
//
// > Hex (bytes 0-11)                                | ASCII
// > ------------------------------------------------+------------------
// > 48 65 6c 6c 6f 20 57 6f 72 6c 64 21             | Hello World!
// >              ^-----^                            |     ^-^
//
// If start
func (s InspectableHexDump) FormatWithHighlightedOffsets(startOffset, endOffset int) string {
	if endOffset < startOffset {
		panic("Codecrafters Internal Error - Start offset larger than end offset in InspectableHexDump")
	}

	// We only point to the start byte if the range is large.
	// Most of the kafka values fit within one line of hexdump. Field which fail assertion
	// are shown this way. See fixtures (mismatched correlation ID/CRC32 fail) for the result.
	// This also helps display cases like where decoded strings span multiple-lines (cases like where string length is
	// wrong and is an unusually large number, but the string following it is correct) in a readable manner.
	if (endOffset - startOffset) > 10 {
		endOffset = startOffset
	}

	s = s.TruncateAroundOffset(startOffset)

	lines := []string{}
	lines = append(lines, s.FormattedStringWithHeading())

	offsetPointerLine := ""

	if startOffset == endOffset {
		// Single offset highlighting: just show ^
		offsetPointerLine += strings.Repeat(" ", s.getOffsetInHexdump(startOffset)) + "^"

		diff := s.getOffsetInAsciiString(startOffset) - len(offsetPointerLine)
		offsetPointerLine += strings.Repeat(" ", diff) + "^"
	} else {
		// Range highlighting: show ^-----^ style
		startHexPosition := s.getOffsetInHexdump(startOffset)
		endHexPosition := s.getOffsetInHexdump(endOffset)

		// Hex section highlighting
		offsetPointerLine += strings.Repeat(" ", startHexPosition) + "^"
		if endHexPosition > startHexPosition {
			offsetPointerLine += strings.Repeat("-", endHexPosition-startHexPosition-1) + "^"
		}

		// ASCII section highlighting
		startAsciiPosition := s.getOffsetInAsciiString(startOffset)
		endAsciiPosition := s.getOffsetInAsciiString(endOffset)

		diff := startAsciiPosition - len(offsetPointerLine)
		offsetPointerLine += strings.Repeat(" ", diff) + "^"

		if endAsciiPosition > startAsciiPosition {
			offsetPointerLine += strings.Repeat("-", endAsciiPosition-startAsciiPosition-1) + "^"
		}
	}

	lines = append(lines, offsetPointerLine)
	return strings.Join(lines, "\n")
}

func (s InspectableHexDump) FormattedString() string {
	var formattedHexdump strings.Builder
	var currentLineHexSectionContents strings.Builder
	var currentLineAsciiSectionContents strings.Builder

	for i, b := range s.bytes {
		if i%16 == 0 && i != 0 {
			formattedHexdump.WriteString(currentLineHexSectionContents.String() + "| " + currentLineAsciiSectionContents.String() + "\n")
			currentLineAsciiSectionContents.Reset()
			currentLineHexSectionContents.Reset()
		}

		currentLineHexSectionContents.WriteString(fmt.Sprintf("%02x ", b))

		// If the character isn't printable, we print . instead
		if b >= 32 && b <= 126 {
			currentLineAsciiSectionContents.WriteByte(b)
		} else {
			currentLineAsciiSectionContents.WriteByte('.')
		}
	}

	if currentLineHexSectionContents.Len() > 0 || currentLineAsciiSectionContents.Len() > 0 {
		// Pad the hex section if necessary
		if (len(s.bytes) % 16) != 0 {
			padding := 16 - (len(s.bytes) % 16)

			for i := 0; i < padding; i++ {
				currentLineHexSectionContents.WriteString("   ")
			}
		}

		formattedHexdump.WriteString(currentLineHexSectionContents.String() + "| " + currentLineAsciiSectionContents.String())
	}

	return formattedHexdump.String()
}

func (s InspectableHexDump) FormattedStringWithHeading() string {
	byteRangeStart := 0
	byteRangeEnd := len(s.bytes) - 1

	if s.truncationStartIndex != 0 {
		byteRangeStart += s.truncationStartIndex
		byteRangeEnd += s.truncationStartIndex
	}

	heading := fmt.Sprintf("Hex (bytes %d-%d)", byteRangeStart, byteRangeEnd)
	prefixLength := 48
	asciiLength := 16 + 2
	heading += strings.Repeat(" ", prefixLength-len(heading)) + "| ASCII\n"

	separator := strings.Repeat("-", prefixLength) + "+" + strings.Repeat("-", asciiLength) + "\n"
	return heading + separator + s.FormattedString()
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

func (s InspectableHexDump) getOffsetInHexdump(byteOffset int) int {
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

func (s InspectableHexDump) getOffsetInAsciiString(byteOffset int) int {
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
