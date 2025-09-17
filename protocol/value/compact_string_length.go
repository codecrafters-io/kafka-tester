package value

import "fmt"

type CompactStringLength struct {
	Value uint64
}

func (v CompactStringLength) String() string {
	switch v.Value {
	case 0:
		return "0 (NULL STRING)"
	case 1:
		// Actual length is still 0
		return "0 (EMPTY STRING)"
	}
	return fmt.Sprintf("%d", v.ActualLength())
}

func (v CompactStringLength) ActualLength() uint64 {
	if v.Value == 0 || v.Value == 1 {
		return 0
	}

	return v.Value - 1
}

func (v CompactStringLength) GetType() string {
	return "COMPACT_STRING_LENGTH"
}
