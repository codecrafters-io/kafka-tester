package encoder

// PacketEncoder is the interface providing helpers for writing with Kafka's encoding rules.
// Types implementing Encoder only need to worry about calling methods like PutString,
// not about how a string is represented in Kafka.
type PacketEncoder interface {
	// Primitives
	PutInt8(in int8)
	PutInt16(in int16)
	PutInt32(in int32)
	PutInt64(in int64)
	PutVarint(in int64)
	PutUVarint(in uint64)
	PutFloat64(in float64)
	PutCompactArrayLength(in int)
	PutArrayLength(in int) error
	PutBool(in bool)

	// Collections
	PutBytes(in []byte) error
	PutVarintBytes(in []byte) error
	PutCompactBytes(in []byte) error
	PutRawBytes(in []byte) error
	PutCompactString(in string) error
	PutNullableCompactString(in *string) error
	PutString(in string) error
	PutNullableString(in *string) error
	PutStringArray(in []string) error
	PutCompactInt32Array(in []int32) error
	PutNullableCompactInt32Array(in []int32) error
	PutInt32Array(in []int32) error
	PutInt64Array(in []int64) error
	PutEmptyTaggedFieldArray()

	// Provide the current offset to record the batch size metric
	Offset() int
}
