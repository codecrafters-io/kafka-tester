package value

import "fmt"

type Int64 struct {
	Value int64
}

func (v Int64) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Int64) GetType() string {
	return "INT64"
}

type AugmentedInt64 struct {
	KafkaValue  Int64
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedInt64) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedInt64) GetPath() string {
	return a.Path
}

func (a AugmentedInt64) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedInt64) GetEndOffset() int {
	return a.EndOffset
}
