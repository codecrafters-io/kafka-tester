package response_assertions

import "github.com/codecrafters-io/kafka-tester/internal/field"

type SingleFieldAssertionError struct {
	Error       error
	StartOffset int
	EndOffset   int
}

func NewSingleFieldAssertionErrorFromFieldAndError(field field.Field, err error) *SingleFieldAssertionError {
	if err == nil {
		return nil
	}

	return &SingleFieldAssertionError{
		Error:       err,
		StartOffset: field.StartOffset,
		EndOffset:   field.EndOffset,
	}
}
