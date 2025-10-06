package value

type CompactNullableString struct {
	Value *string
}

func (v CompactNullableString) String() string {
	if v.Value == nil {
		return "NULL"
	}
	return *v.Value
}

func (v CompactNullableString) GetType() string {
	return "COMPACT_NULLABLE_STRING"
}

type AugmentedCompactNullableString struct {
	KafkaValue  CompactNullableString
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedCompactNullableString) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedCompactNullableString) GetPath() string {
	return a.Path
}

func (a AugmentedCompactNullableString) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedCompactNullableString) GetEndOffset() int {
	return a.EndOffset
}
