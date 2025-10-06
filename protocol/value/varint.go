package value

import "fmt"

type Varint struct {
	Value int64
}

func (v Varint) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Varint) GetType() string {
	return "VARINT"
}

type AugmentedVarint struct {
	KafkaValue  Varint
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedVarint) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedVarint) GetPath() string {
	return a.Path
}

func (a AugmentedVarint) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedVarint) GetEndOffset() int {
	return a.EndOffset
}
