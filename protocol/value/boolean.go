package value

type Boolean struct {
	Value bool
}

func (v Boolean) String() string {
	if !v.Value {
		return "False"
	}
	return "True"
}

func (v Boolean) GetType() string {
	return "BOOLEAN"
}
