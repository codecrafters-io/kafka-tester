package value

import "fmt"

type RawBytes struct {
	Value []byte
}

func (v RawBytes) String() string {
	if v.Value == nil {
		return ""
	}
	return fmt.Sprintf("%v -> UTF-8: (%s)", v.Value, string(v.Value))
}

func (v RawBytes) GetType() string {
	return "RAW_BYTES"
}

type AugmentedRawBytes struct {
	KafkaValue  RawBytes
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedRawBytes) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedRawBytes) GetPath() string {
	return a.Path
}

func (a AugmentedRawBytes) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedRawBytes) GetEndOffset() int {
	return a.EndOffset
}
