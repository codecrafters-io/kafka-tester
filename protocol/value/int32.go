package value

import "fmt"

type Int32 struct {
	Value int32
}

func (v Int32) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Int32) GetType() string {
	return "INT32"
}

type AugmentedInt32 struct {
	KafkaValue  Int32
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedInt32) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedInt32) GetPath() string {
	return a.Path
}

func (a AugmentedInt32) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedInt32) GetEndOffset() int {
	return a.EndOffset
}
