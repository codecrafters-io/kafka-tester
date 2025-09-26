package field_tree_printer

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field"
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FieldTreePrinter struct {
	Fields []field.Field
	Logger *logger.Logger

	currentIndentationLevel int
	lastPrintedFieldPath    field_path.FieldPath
}

func (p FieldTreePrinter) PrintForFieldAssertionError(errorPath field_path.FieldPath) {
	p.currentIndentationLevel = 0
	p.lastPrintedFieldPath = field_path.NewFieldPath("")

	for _, field := range p.Fields {
		p.printNodesLeadingTo(field.Path, p.Logger.Infof)

		if errorPath.Is(field.Path) {
			p.Logger.Infof("%s❌ %s (%s)", p.buildIndentPrefix(), field.Path.LastSegment(), field.Value.String())
			return
		} else {
			p.Logger.Infof("%s- %s (%s)", p.buildIndentPrefix(), field.Path.LastSegment(), field.Value.String())
			p.lastPrintedFieldPath = field.Path
		}
	}

	panic(fmt.Sprintf("Error field path '%s' not found in FieldTreePrinter.Fields", errorPath))
}

func (p FieldTreePrinter) PrintForDecodeError(errorPath field_path.FieldPath) {
	p.currentIndentationLevel = 0
	p.lastPrintedFieldPath = field_path.NewFieldPath("")

	for _, field := range p.Fields {
		p.printNodesLeadingTo(field.Path, p.Logger.Infof)

		if errorPath.Is(field.Path) {
			p.Logger.Infof("%s❌ %s (%s)", p.buildIndentPrefix(), field.Path.LastSegment(), field.Value.String())
			return
		} else {
			p.Logger.Infof("%s- %s (%s)", p.buildIndentPrefix(), field.Path.LastSegment(), field.Value.String())
			p.lastPrintedFieldPath = field.Path
		}
	}

	p.printNodesLeadingTo(errorPath, p.Logger.Infof)
	p.Logger.Errorf("%s❌ %s (%s)", p.buildIndentPrefix(), errorPath.LastSegment(), "decode error")
}

func (p FieldTreePrinter) PrintForDebugLogs() {
	p.currentIndentationLevel = 0
	p.lastPrintedFieldPath = field_path.NewFieldPath("")

	for _, field := range p.Fields {
		p.printNodesLeadingTo(field.Path, p.Logger.Debugf)
		p.Logger.Debugf("%s- %s (%s)", p.buildIndentPrefix(), field.Path.LastSegment(), field.Value.String())
		p.lastPrintedFieldPath = field.Path
	}
}

func (p FieldTreePrinter) buildIndentPrefix() string {
	return strings.Repeat(" ", p.currentIndentationLevel*2)
}

func (p *FieldTreePrinter) printNodesLeadingTo(nextPath field_path.FieldPath, logFn func(string, ...interface{})) {
	if nextPath.IsSiblingOf(p.lastPrintedFieldPath) {
		// If the next path is a sibling, we don't need to print nodes or adjust indentation level
	} else if nextPath.IsDescendantOf(p.lastPrintedFieldPath) {
		// If the next path is a descendant, we indent and print each ancestor between
		for _, descendantPath := range p.lastPrintedFieldPath.DescendantsUntil(nextPath) {
			logFn("%s- %s", p.buildIndentPrefix(), descendantPath.LastSegment())
			p.currentIndentationLevel++
		}
	} else {
		// If it's neither a sibling or a descendant, we reset indentation level until the common ancestor
		commonAncestor := nextPath.CommonAncestor(p.lastPrintedFieldPath)
		p.currentIndentationLevel = len(commonAncestor.Segments)
		p.lastPrintedFieldPath = commonAncestor
		p.printNodesLeadingTo(nextPath, logFn)
	}
}
