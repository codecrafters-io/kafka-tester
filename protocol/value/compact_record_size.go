package value

import "fmt"

type CompactRecordSize struct {
	Value uint64
}

func (v CompactRecordSize) String() string {
	switch v.Value {
	case 0:
		return "0 (Null record)"
	case 1:
		// Actual size is still 0
		return "1 (Record size(0) + 1)"
	}
	return fmt.Sprintf("%d (Record size(%d) + 1)", v.Value, v.ActualSize())
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

type AugmentedCompactRecordSize struct {
	KafkaValue  CompactRecordSize
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedCompactRecordSize) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedCompactRecordSize) GetPath() string {
	return a.Path
}

func (a AugmentedCompactRecordSize) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedCompactRecordSize) GetEndOffset() int {
	return a.EndOffset
}
