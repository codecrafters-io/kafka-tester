package value

import "fmt"

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
