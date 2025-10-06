package value

type CompactString struct {
	Value string
}

func (v CompactString) String() string {
	// Print quotes in case of empty string
	if v.Value == "" {
		return `""`
	}
	return string(v.Value)
}

func (v CompactString) GetType() string {
	return "COMPACT_STRING"
}

type AugmentedCompactString struct {
	KafkaValue  CompactString
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedCompactString) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedCompactString) GetPath() string {
	return a.Path
}

func (a AugmentedCompactString) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedCompactString) GetEndOffset() int {
	return a.EndOffset
}
