package value

import "fmt"

type UnsignedVarint struct {
	Value uint64
}

func (v UnsignedVarint) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v UnsignedVarint) GetType() string {
	return "UNSIGNED_VARINT"
}

type AugmentedUnsignedVarint struct {
	KafkaValue  UnsignedVarint
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedUnsignedVarint) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedUnsignedVarint) GetPath() string {
	return a.Path
}

func (a AugmentedUnsignedVarint) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedUnsignedVarint) GetEndOffset() int {
	return a.EndOffset
}
