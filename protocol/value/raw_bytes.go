package value

import "fmt"

type RawBytes struct {
	Value []byte
}

func (v RawBytes) String() string {
	if v.Value == nil {
		return ""
	}
	return fmt.Sprintf("%v", v.Value)
}

func (v RawBytes) GetType() string {
	return "RAW_BYTES"
}
