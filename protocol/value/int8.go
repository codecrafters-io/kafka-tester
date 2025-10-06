package value

import "fmt"

type Int8 struct {
	Value int8
}

func (v Int8) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Int8) GetType() string {
	return "INT8"
}

type AugmentedInt8 struct {
	KafkaValue  Int8
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedInt8) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedInt8) GetPath() string {
	return a.Path
}

func (a AugmentedInt8) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedInt8) GetEndOffset() int {
	return a.EndOffset
}
