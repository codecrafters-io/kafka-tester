package value

// UUID represents uuid of the format given by utils.DecodeUUID
type UUID struct {
	Value string
}

func (v UUID) String() string {
	return v.Value
}

func (v UUID) GetType() string {
	return "UUID"
}
