package decoder

// DecoderI is the interface providing helpers for reading with Kafka's encoding rules.
// Types implementing Decoder only need to worry about calling methods like GetString,
// not about how a string is represented in Kafka.
type DecoderI interface {
	// Primitives

	GetInt8() (int8, error)
	GetInt16() (int16, error)
	GetInt32() (int32, error)
	GetInt64() (int64, error)
	GetVarint() (int64, error)
	GetUVarint() (uint64, error)
	GetFloat64() (float64, error)
	GetArrayLength() (int, error)
	GetCompactArrayLength() (int, error)
	GetBool() (bool, error)
	GetEmptyTaggedFieldArray() (int, error)

	// Collections

	GetBytes() ([]byte, error)
	GetVarintBytes() ([]byte, error)
	GetCompactBytes() ([]byte, error)
	GetRawBytes(length int) ([]byte, error)
	GetString() (string, error)
	GetNullableString() (*string, error)
	GetCompactString() (string, error)
	GetCompactNullableString() (*string, error)
	GetCompactInt32Array() ([]int32, error)
	GetInt32Array() ([]int32, error)
	GetInt64Array() ([]int64, error)
	GetStringArray() ([]string, error)

	// Subsets

	Remaining() int
	GetSubset(length int) (DecoderI, error)
	Peek(offset, length int) (DecoderI, error) // similar to GetSubset, but it doesn't advance the offset
	PeekInt8(offset int) (int8, error)         // similar to peek, but just one byte
}
