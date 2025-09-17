package value

import "fmt"

type CompactRecordSize struct {
	Value uint64
}

func (v CompactRecordSize) String() string {
	switch v.Value {
	case 0:
		return "0 (NULL RECORD)"
	case 1:
		return "0 (EMPTY RECORD)"
	}
	return fmt.Sprintf("%d", v.ActualSize())
}

func (v CompactRecordSize) ActualSize() uint64 {
	if v.Value == 0 || v.Value == 1 {
		return 0
	}

	return v.Value - 1
}

func (v CompactRecordSize) GetType() string {
	return "COMPACT_RECORD_SIZE"
}
