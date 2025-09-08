package field_tree_printer

import (
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FieldTreePrinter struct {
	DecodedFields []field_decoder.Field
	ErrorPath     *field_path.FieldPath // Can be nil
	Logger        *logger.Logger

	lastPrintedFieldPath    field_path.FieldPath
	currentIndentationLevel int
}

func (p FieldTreePrinter) Print() {
	p.lastPrintedFieldPath = field_path.NewFieldPath("")
	p.currentIndentationLevel = 0

	for _, decodedField := range p.DecodedFields {
		p.printNodesLeadingTo(decodedField.Path)

		if p.ErrorPath != nil && p.ErrorPath.Is(decodedField.Path) {
			p.Logger.Infof("%s❌ %s (%s)", p.buildIndentPrefix(), decodedField.Path.LastSegment(), decodedField.Value.String())
			return
		} else {
			p.Logger.Infof("%s- %s (%s)", p.buildIndentPrefix(), decodedField.Path.LastSegment(), decodedField.Value.String())
			p.lastPrintedFieldPath = decodedField.Path
		}
	}

	// If we're here and ErrorPath is not nil, the error must be in a field that wasn't decoded
	if p.ErrorPath != nil {
		p.printNodesLeadingTo(*p.ErrorPath)
		p.Logger.Infof("%s❌ %s (decode error)", p.buildIndentPrefix(), p.ErrorPath.LastSegment())
	}
}

func (p FieldTreePrinter) buildIndentPrefix() string {
	return strings.Repeat(" ", p.currentIndentationLevel*2)
}

func (p *FieldTreePrinter) printNodesLeadingTo(nextPath field_path.FieldPath) {
	if nextPath.IsSiblingOf(p.lastPrintedFieldPath) {
		// If the next path is a sibling, we don't need to print nodes or adjust indentation level
	} else if nextPath.IsDescendantOf(p.lastPrintedFieldPath) {
		// If the next path is a descendant, we indent and print each ancestor between
		for _, descendantPath := range p.lastPrintedFieldPath.DescendantsUntil(nextPath) {
			p.Logger.Infof("%s- %s", p.buildIndentPrefix(), descendantPath.LastSegment())
			p.currentIndentationLevel++
		}
	} else {
		// If it's neither a sibling or a descendant, we reset indentation level until the common ancestor
		commonAncestor := nextPath.CommonAncestor(p.lastPrintedFieldPath)
		p.currentIndentationLevel = len(commonAncestor.Segments)
		p.lastPrintedFieldPath = commonAncestor
		p.printNodesLeadingTo(nextPath)
	}
}
