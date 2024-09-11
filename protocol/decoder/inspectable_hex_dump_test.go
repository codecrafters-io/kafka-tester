package decoder

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormattedString(t *testing.T) {
	bytes := []byte("Hello World!")
	ibs := NewInspectableHexDump(bytes)
	assert.Equal(t, `48 65 6c 6c 6f 20 57 6f 72 6c 64 21             | Hello World!`, ibs.FormattedString())
}

func TestGetOffsetInHexdump(t *testing.T) {
	bytes := []byte("+OK\r\n")
	ibs := NewInspectableHexDump(bytes)

	assert.Equal(t, 1, ibs.GetOffsetInHexdump(0))  // For 0, we put caret at 0
	assert.Equal(t, 4, ibs.GetOffsetInHexdump(1))  // For 1, we put caret at end of 2nd byte
	assert.Equal(t, 7, ibs.GetOffsetInHexdump(2))  // For 2, we put caret at end of 3rd byte
	assert.Equal(t, 10, ibs.GetOffsetInHexdump(3)) // For 3, we put caret at end of 4th byte
	assert.Equal(t, 13, ibs.GetOffsetInHexdump(4)) // For 4, we put caret at end of 5th byte
}

func TestGetOffsetInAsciiString(t *testing.T) {
	bytes := []byte("+OK\r\n")
	ibs := NewInspectableHexDump(bytes)

	assert.Equal(t, 50, ibs.GetOffsetInAsciiString(0)) //
	assert.Equal(t, 51, ibs.GetOffsetInAsciiString(1)) //
	assert.Equal(t, 52, ibs.GetOffsetInAsciiString(2)) //
	assert.Equal(t, 53, ibs.GetOffsetInAsciiString(3)) //
	assert.Equal(t, 54, ibs.GetOffsetInAsciiString(4)) //
}

func TestTruncateAroundOffset(t *testing.T) {
	bytes := []byte("+OK\r\n")
	ibs := NewInspectableHexDump(bytes)

	assert.Equal(t, "+OK\r\n", ibs.TruncateAroundOffset(4).String())
	assert.Equal(t, "+OK\r\n", ibs.TruncateAroundOffset(5).String())
	assert.Equal(t, "OK\r\n", ibs.TruncateAroundOffset(6).String())

	bytes = []byte{}

	for i := 0; i < 10; i++ {
		bytes = append(bytes, []byte(fmt.Sprintf("helloworld%d", i))...)
	}

	ibs = NewInspectableHexDump(bytes)
	// Our window is 10 bytes only.
	// Hexdump + ascii will take a lot more tho.
	assert.Equal(t, "helloworld5hello", ibs.TruncateAroundOffset(60).String())
	assert.Equal(t, "68 65 6c 6c 6f 77 6f 72 6c 64 35 68 65 6c 6c 6f | helloworld5hello", ibs.TruncateAroundOffset(60).FormattedString())
}

func TestFormatWithHighlightedOffset(t *testing.T) {
	bytes := []byte("Hello World!")
	ibs := NewInspectableHexDump(bytes)
	highlightOffset := 5
	highlightText := "error"

	expected := strings.TrimSpace(`
Hex (bytes 0-12)                                | ASCII
48 65 6c 6c 6f 20 57 6f 72 6c 64 21             | Hello World!
                ^ error
                                                       ^ error
	`)
	result := ibs.FormatWithHighlightedOffset(highlightOffset, highlightText)

	assert.Equal(t, expected, result)
}

func TestFormatWithHighlightedOffset2(t *testing.T) {
	bytes := []byte("Helllo Earth & Moooon!")
	ibs := NewInspectableHexDump(bytes)
	highlightOffset := 10
	highlightText := "this is the error, innit"

	expected := strings.TrimSpace(`
Hex (bytes 5-16)                                | ASCII
6f 20 45 61 72 74 68 20 26 20 4d 6f 6f 6f 6f 6e | o Earth & Moooon
                ^ this is the error, innit
                                                       ^ this is the error, innit
	`)
	result := ibs.FormatWithHighlightedOffset(highlightOffset, highlightText)

	assert.Equal(t, expected, result)
}
