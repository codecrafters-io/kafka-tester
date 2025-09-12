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
