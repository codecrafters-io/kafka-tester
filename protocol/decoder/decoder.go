package decoder

// PacketDecoder is the interface providing helpers for reading with Kafka's encoding rules.
// Types implementing Decoder only need to worry about calling methods like GetString,
// not about how a string is represented in Kafka.
type packetDecoder interface {
	// Primitives
	getInt8() (int8, error)
	getInt16() (int16, error)
	getInt32() (int32, error)
	getInt64() (int64, error)
	getVarint() (int64, error)
	getUVarint() (uint64, error)
	getFloat64() (float64, error)
	getArrayLength() (int, error)
	getCompactArrayLength() (int, error)
	getBool() (bool, error)
	getEmptyTaggedFieldArray() (int, error)

	// Collections
	getBytes() ([]byte, error)
	getVarintBytes() ([]byte, error)
	getCompactBytes() ([]byte, error)
	getRawBytes(length int) ([]byte, error)
	getString() (string, error)
	getNullableString() (*string, error)
	getCompactString() (string, error)
	getCompactNullableString() (*string, error)
	getCompactInt32Array() ([]int32, error)
	getInt32Array() ([]int32, error)
	getInt64Array() ([]int64, error)
	getStringArray() ([]string, error)

	// Subsets
	remaining() int
	getSubset(length int) (packetDecoder, error)
	peek(offset, length int) (packetDecoder, error) // similar to getSubset, but it doesn't advance the offset
	peekInt8(offset int) (int8, error)              // similar to peek, but just one byte
}
