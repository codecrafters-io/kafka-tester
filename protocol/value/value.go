package value

import (
	"fmt"
)

const (
	INT16      = "INT16"
	INT32      = "INT32"
	TAG_BUFFER = "TAG_BUFFER"
)

type KafkaProtocolValue interface {
	String() string
	GetType() string
}

type Int16 struct {
	Value int16
}

func (v Int16) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Int16) GetType() string {
	return "INT16"
}

type Int32 struct {
	Value int32
}

func (v Int32) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Int32) GetType() string {
	return "INT32"
}

type UnsignedVarint struct {
	Value uint64
}

func (v UnsignedVarint) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v UnsignedVarint) GetType() string {
	return "UNSIGNED_VARINT"
}

// TODO[PaulRefactor]: Check if this is _actually_ a primitive Kafka type
type CompactArrayLength struct {
	Value uint64
}

func (v CompactArrayLength) String() string {
	switch v.Value {
	case 0:
		return "0 (NULL ARRAY)"
	case 1:
		return "1 (EMPTY ARRAY)"
	}
	return fmt.Sprintf("%d", v.Value-1)
}

func (v CompactArrayLength) ActualLength() uint64 {
	if v.Value == 0 {
		return v.Value
	}

	if v.Value == 1 {
		return 0
	}

	return v.Value - 1
}

func (v CompactArrayLength) GetType() string {
	return "UNSIGNED_VARINT"
}
