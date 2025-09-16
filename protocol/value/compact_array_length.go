package value

import "fmt"

type CompactArrayLength struct {
	Value uint64
}

func NewCompactArrayLength[T any](array []T) CompactArrayLength {
	if array == nil {
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
		return "0 (NULL ARRAY)"
	case 1:
		// Actual length is still 0
		return "0 (EMPTY ARRAY)"
	}
	return fmt.Sprintf("%d", v.ActualLength())
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
