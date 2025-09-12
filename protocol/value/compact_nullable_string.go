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
