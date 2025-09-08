package value

import "fmt"

type UnsignedVarint struct {
	Value uint64
}

func (v UnsignedVarint) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v UnsignedVarint) GetType() string {
	return "UNSIGNED_VARINT"
}
