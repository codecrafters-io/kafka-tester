package field_values_printer

import (
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FieldValuesPrinter struct {
	AssertionError     error
	AssertionErrorPath field_path.FieldPath
	DecodeError        *field_decoder.FieldDecoderError
	DecodedFields      []field_decoder.Field
	Logger             *logger.Logger
}

func (r FieldValuesPrinter) Print() {
	lastPrintedFieldPath := field_path.NewFieldPath("")
	currentIndentationLevel := 0

	buildIndentPrefix := func() string {
		return strings.Repeat(" ", currentIndentationLevel*2)
	}

	for _, decodedField := range r.DecodedFields {
		if decodedField.Path.IsSiblingOf(lastPrintedFieldPath) {
			// If the path is a sibling, we don't need to adjust indentation level
		} else if decodedField.Path.IsDescendantOf(lastPrintedFieldPath) {
			// If it's a descendant, we indent and print each ancestor between
			for _, descendantPath := range lastPrintedFieldPath.DescendantsUntil(decodedField.Path) {
				r.Logger.Infof("%s- %s", buildIndentPrefix(), descendantPath.LastSegment())
				currentIndentationLevel++
			}
		} else {
			// If it's neither a sibling or a descendant, we reset indentation level until the common ancestor
			commonAncestor := decodedField.Path.CommonAncestor(lastPrintedFieldPath)
			currentIndentationLevel = len(commonAncestor.Segments)
			lastPrintedFieldPath = commonAncestor

			for _, descendantPath := range lastPrintedFieldPath.DescendantsUntil(decodedField.Path) {
				r.Logger.Infof("%s- %s", buildIndentPrefix(), descendantPath.LastSegment())
				currentIndentationLevel++
			}
		}

		if r.AssertionError != nil && r.AssertionErrorPath.Is(decodedField.Path) {
			r.Logger.Infof("%s❌ %s (%s)", buildIndentPrefix(), decodedField.Path.LastSegment(), decodedField.Value.String())
			break
		}

		if r.DecodeError != nil && r.DecodeError.Path.Is(decodedField.Path) {
			r.Logger.Infof("%s- %s (Decode Error)", buildIndentPrefix(), decodedField.Path.LastSegment())
			break
		}

		r.Logger.Infof("%s- %s (%s)", buildIndentPrefix(), decodedField.Path.LastSegment(), decodedField.Value.String())
		lastPrintedFieldPath = decodedField.Path
	}
}
