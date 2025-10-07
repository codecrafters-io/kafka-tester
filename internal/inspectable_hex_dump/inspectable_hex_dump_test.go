package inspectable_hex_dump

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type formatWithHighlightedOffsetTestCase struct {
	bytes           []byte
	highlightOffset int
	expected        string
}

func TestFormatWithHighlightedOffset(t *testing.T) {
	testCases := []formatWithHighlightedOffsetTestCase{
		{
			bytes:           []byte("Hello World!"),
			highlightOffset: 5,
			expected: strings.TrimSpace(`
Hex (bytes 0-11)                                | ASCII
------------------------------------------------+------------------
48 65 6c 6c 6f 20 57 6f 72 6c 64 21             | Hello World!
                ^                                      ^
			`),
		},
		{
			bytes:           []byte("Helllo Earth & Moooon!"),
			highlightOffset: 10,
			expected: strings.TrimSpace(`
Hex (bytes 5-20)                                | ASCII
------------------------------------------------+------------------
6f 20 45 61 72 74 68 20 26 20 4d 6f 6f 6f 6f 6e | o Earth & Moooon
                ^                                      ^
			`),
		},
		{
			bytes:           []byte("Long string with more than 16 bytes!"),
			highlightOffset: 20,
			expected: strings.TrimSpace(`
Hex (bytes 15-30)                               | ASCII
------------------------------------------------+------------------
68 20 6d 6f 72 65 20 74 68 61 6e 20 31 36 20 62 | h more than 16 b
                ^                                      ^
		`),
		},
	}

	for _, testCase := range testCases {
		t.Run(string(testCase.bytes), func(t *testing.T) {
			ibs := NewInspectableHexDump(testCase.bytes)
			result := ibs.FormatWithHighlightedRange(testCase.highlightOffset, testCase.highlightOffset)
			assert.Equal(t, testCase.expected, result)
		})
	}
}

type formatWithHighlightedRangeTestCase struct {
	bytes       []byte
	startOffset int
	endOffset   int
	expected    string
}

func TestFormatWithHighlightedRange(t *testing.T) {
	testCases := []formatWithHighlightedRangeTestCase{
		{
			bytes:       []byte("Hello World!"),
			startOffset: 4,
			endOffset:   7,
			expected: strings.TrimSpace(`
Hex (bytes 0-11)                                | ASCII
------------------------------------------------+------------------
48 65 6c 6c 6f 20 57 6f 72 6c 64 21             | Hello World!
             ^--------^                               ^--^
			`),
		},
		{
			bytes:       []byte("Hello World!"),
			startOffset: 0,
			endOffset:   2,
			expected: strings.TrimSpace(`
Hex (bytes 0-11)                                | ASCII
------------------------------------------------+------------------
48 65 6c 6c 6f 20 57 6f 72 6c 64 21             | Hello World!
 ^-----^                                          ^-^
			`),
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s_%d_%d", string(testCase.bytes), testCase.startOffset, testCase.endOffset), func(t *testing.T) {
			ibs := NewInspectableHexDump(testCase.bytes)
			result := ibs.FormatWithHighlightedRange(testCase.startOffset, testCase.endOffset)
			assert.Equal(t, testCase.expected, result)
		})
	}
}
