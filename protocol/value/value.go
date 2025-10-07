package value

import "fmt"

type KafkaProtocolValue interface {
	String() string
	GetType() string
}

func MustBeBoolean(value KafkaProtocolValue) Boolean {
	if value.GetType() != "BOOLEAN" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not BOOLEAN", value.GetType()))
	}
	return value.(Boolean)
}

func MustBeInt8(value KafkaProtocolValue) Int8 {
	if value.GetType() != "INT8" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not INT8", value.GetType()))
	}
	return value.(Int8)
}

func MustBeInt16(value KafkaProtocolValue) Int16 {
	if value.GetType() != "INT16" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not INT16", value.GetType()))
	}
	return value.(Int16)
}

func MustBeInt32(value KafkaProtocolValue) Int32 {
	if value.GetType() != "INT32" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not INT32", value.GetType()))
	}
	return value.(Int32)
}

func MustBeInt64(value KafkaProtocolValue) Int64 {
	if value.GetType() != "INT64" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not INT64", value.GetType()))
	}
	return value.(Int64)
}

func MustBeVarint(value KafkaProtocolValue) Varint {
	if value.GetType() != "VARINT" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not VARINT", value.GetType()))
	}
	return value.(Varint)
}

func MustBeUnsignedVarint(value KafkaProtocolValue) UnsignedVarint {
	if value.GetType() != "UNSIGNED_VARINT" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not UNSIGNED_VARINT", value.GetType()))
	}
	return value.(UnsignedVarint)
}

func MustBeString(value KafkaProtocolValue) String {
	if value.GetType() != "STRING" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not STRING", value.GetType()))
	}
	return value.(String)
}

func MustBeCompactString(value KafkaProtocolValue) CompactString {
	if value.GetType() != "COMPACT_STRING" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not COMPACT_STRING", value.GetType()))
	}
	return value.(CompactString)
}

func MustBeCompactNullableString(value KafkaProtocolValue) CompactNullableString {
	if value.GetType() != "COMPACT_NULLABLE_STRING" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not COMPACT_NULLABLE_STRING", value.GetType()))
	}
	return value.(CompactNullableString)
}

func MustBeRawBytes(value KafkaProtocolValue) RawBytes {
	if value.GetType() != "RAW_BYTES" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not RAW_BYTES", value.GetType()))
	}
	return value.(RawBytes)
}

func MustBeUUID(value KafkaProtocolValue) UUID {
	if value.GetType() != "UUID" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not UUID", value.GetType()))
	}
	return value.(UUID)
}

func MustBeCompactStringLength(value KafkaProtocolValue) CompactStringLength {
	if value.GetType() != "COMPACT_STRING_LENGTH" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not COMPACT_STRING_LENGTH", value.GetType()))
	}
	return value.(CompactStringLength)
}

func MustBeCompactArrayLength(value KafkaProtocolValue) CompactArrayLength {
	if value.GetType() != "COMPACT_ARRAY_LENGTH" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not COMPACT_ARRAY_LENGTH", value.GetType()))
	}
	return value.(CompactArrayLength)
}

func MustBeCompactRecordSize(value KafkaProtocolValue) CompactRecordSize {
	if value.GetType() != "COMPACT_RECORD_SIZE" {
		panic(fmt.Sprintf("Codecrafters Internal Error - Value of type %s is not COMPACT_RECORD_SIZE", value.GetType()))
	}
	return value.(CompactRecordSize)
}
