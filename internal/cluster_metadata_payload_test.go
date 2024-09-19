package internal

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/stretchr/testify/assert"
)

func TestBeginTxnRecord(t *testing.T) {
	hexdump := "01170001001212426f6f747374726170207265636f726473"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	decoder := decoder.RealDecoder{}
	decoder.Init(b)

	frame, err := decoder.GetInt8()
	fmt.Printf("frame: %d\n", frame)
	assert.NoError(t, err)

	typ, err := decoder.GetInt8()
	fmt.Printf("typ: %d\n", typ)
	assert.NoError(t, err)

	version, err := decoder.GetInt8()
	fmt.Printf("version: %d\n", version)
	assert.NoError(t, err)

	tagFieldCount, err := decoder.GetUnsignedVarint()
	fmt.Printf("tagFieldCount: %d\n", tagFieldCount)
	assert.NoError(t, err)

	for i := 0; i < int(tagFieldCount); i++ {
		tagType, err := decoder.GetUnsignedVarint()
		fmt.Printf("tagType: %d\n", tagType)
		assert.NoError(t, err)

		tagLength, err := decoder.GetUnsignedVarint()
		fmt.Printf("tagLength: %d\n", tagLength)
		assert.NoError(t, err)

		stringLength, err := decoder.GetUnsignedVarint()
		fmt.Printf("stringLength: %d\n", stringLength)
		assert.NoError(t, err)

		stringValue, err := decoder.GetRawBytes(int(stringLength) - 1)
		fmt.Printf("stringValue: %s\n", stringValue)
		assert.NoError(t, err)
	}

	assert.Equal(t, 0, int(decoder.Remaining()))
}

func TestFeatureLevelRecord(t *testing.T) {
	hexdump := "010c00116d657461646174612e76657273696f6e001400"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	decoder := decoder.RealDecoder{}
	decoder.Init(b)

	frame, err := decoder.GetInt8()
	fmt.Printf("frame: %d\n", frame)
	assert.NoError(t, err)

	typ, err := decoder.GetInt8()
	fmt.Printf("typ: %d\n", typ)
	assert.NoError(t, err)

	version, err := decoder.GetInt8()
	fmt.Printf("version: %d\n", version)
	assert.NoError(t, err)

	stringLength, err := decoder.GetUnsignedVarint()
	fmt.Printf("stringLength: %d\n", stringLength)
	assert.NoError(t, err)

	stringValue, err := decoder.GetRawBytes(int(stringLength) - 1)
	fmt.Printf("stringValue: %s\n", stringValue)
	assert.NoError(t, err)

	featureLevel, err := decoder.GetInt16()
	fmt.Printf("featureLevel: %d\n", featureLevel)
	assert.NoError(t, err)

	tagFieldCount, err := decoder.GetUnsignedVarint()
	fmt.Printf("tagFieldCount: %d\n", tagFieldCount)
	assert.NoError(t, err)

	assert.Equal(t, 0, int(decoder.Remaining()))
}
