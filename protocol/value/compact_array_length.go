package value

import (
	"fmt"
	"reflect"
)

type CompactArrayLength struct {
	Value uint64
}

func NewCompactArrayLength[T any](array []T) CompactArrayLength {
	// array could be a nil or an interface wrapper around nil in case of empty arrays
	if array == nil || reflect.ValueOf(array).IsNil() {
		return CompactArrayLength{
			Value: 0,
		}
	}

	return CompactArrayLength{
		Value: uint64(len(array) + 1),
	}
}

func (v CompactArrayLength) String() string {
	switch v.Value {
	case 0:
		return "0 (Null Array)"
	case 1:
		// Actual length is still 0
		return "1 (Array length(0) + 1)"
	}
	return fmt.Sprintf("%d (Array length(%d) + 1)", v.Value, v.ActualLength())
}

func (v CompactArrayLength) ActualLength() uint64 {
	if v.Value == 0 || v.Value == 1 {
		return 0
	}

	return v.Value - 1
}

func (v CompactArrayLength) GetType() string {
	return "COMPACT_ARRAY_LENGTH"
}
