package value

import "fmt"

type Int16 struct {
	Value int16
}

func (v Int16) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Int16) GetType() string {
	return "INT16"
}

type AugmentedInt16 struct {
	KafkaValue  Int16
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedInt16) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedInt16) GetPath() string {
	return a.Path
}

func (a AugmentedInt16) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedInt16) GetEndOffset() int {
	return a.EndOffset
}
