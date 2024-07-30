package encoder

// PacketEncoder is the interface providing helpers for writing with Kafka's encoding rules.
// Types implementing Encoder only need to worry about calling methods like PutString,
// not about how a string is represented in Kafka.
type packetEncoder interface {
	// Primitives
	putInt8(in int8)
	putInt16(in int16)
	putInt32(in int32)
	putInt64(in int64)
	putVarint(in int64)
	putUVarint(in uint64)
	putFloat64(in float64)
	putCompactArrayLength(in int)
	putArrayLength(in int) error
	putBool(in bool)

	// Collections
	putBytes(in []byte) error
	putVarintBytes(in []byte) error
	putCompactBytes(in []byte) error
	putRawBytes(in []byte) error
	putCompactString(in string) error
	putNullableCompactString(in *string) error
	putString(in string) error
	putNullableString(in *string) error
	putStringArray(in []string) error
	putCompactInt32Array(in []int32) error
	putNullableCompactInt32Array(in []int32) error
	putInt32Array(in []int32) error
	putInt64Array(in []int64) error
	putEmptyTaggedFieldArray()

	// Provide the current offset to record the batch size metric
	offset() int
}
