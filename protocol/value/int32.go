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
