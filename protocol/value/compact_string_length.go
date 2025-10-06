package value

import "fmt"

type CompactStringLength struct {
	Value uint64
}

func (v CompactStringLength) String() string {
	switch v.Value {
	case 0:
		return "0 (Null string)"
	case 1:
		return "1 (String length(0) + 1)"
	}
	return fmt.Sprintf("%d (String length(%d) + 1)", v.Value, v.ActualLength())
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

type AugmentedCompactStringLength struct {
	KafkaValue  CompactStringLength
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedCompactStringLength) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedCompactStringLength) GetPath() string {
	return a.Path
}

func (a AugmentedCompactStringLength) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedCompactStringLength) GetEndOffset() int {
	return a.EndOffset
}
