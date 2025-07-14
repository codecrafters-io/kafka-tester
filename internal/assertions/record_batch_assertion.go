package assertions

import (
	"fmt"
	"reflect"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type RecordBatchAssertion struct {
	ActualValue   kafkaapi.RecordBatch
	ExpectedValue kafkaapi.RecordBatch
	logger        *logger.Logger
	err           error

	// nil = don't assert this level
	// empty slice = assert all fields (default)
	// non-empty slice = assert with exclusions
	excludedBatchFields  []string
	excludedRecordFields []string
}

func NewRecordBatchAssertion(actualValue kafkaapi.RecordBatch, expectedValue kafkaapi.RecordBatch, logger *logger.Logger) *RecordBatchAssertion {
	return &RecordBatchAssertion{
		ActualValue:          actualValue,
		ExpectedValue:        expectedValue,
		logger:               logger,
		excludedBatchFields:  []string{},
		excludedRecordFields: []string{},
	}
}

func (a *RecordBatchAssertion) ExcludeBatchFields(fields ...string) *RecordBatchAssertion {
	a.excludedBatchFields = fields
	return a
}

func (a *RecordBatchAssertion) ExcludeRecordFields(fields ...string) *RecordBatchAssertion {
	a.excludedRecordFields = fields
	return a
}

func (a *RecordBatchAssertion) SkipRecordFields() *RecordBatchAssertion {
	a.excludedRecordFields = nil
	return a
}

// AssertBatch asserts the contents of the RecordBatch
// Fields asserted by default: BaseOffset, BatchLength, PartitionLeaderEpoch, Magic, CRC, Attributes, 
// LastOffsetDelta, FirstTimestamp, MaxTimestamp, ProducerId, ProducerEpoch, BaseSequence, RecordCount
func (a *RecordBatchAssertion) AssertBatch() *RecordBatchAssertion {
	if a.err != nil {
		return a
	}

	if !Contains(a.excludedBatchFields, "BaseOffset") {
		if a.ActualValue.BaseOffset != a.ExpectedValue.BaseOffset {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "BaseOffset", a.ExpectedValue.BaseOffset, a.ActualValue.BaseOffset)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ BaseOffset: %d", a.ActualValue.BaseOffset)
	}

	if !Contains(a.excludedBatchFields, "BatchLength") {
		if a.ActualValue.BatchLength != a.ExpectedValue.BatchLength {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "BatchLength", a.ExpectedValue.BatchLength, a.ActualValue.BatchLength)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ BatchLength: %d", a.ActualValue.BatchLength)
	}

	if !Contains(a.excludedBatchFields, "PartitionLeaderEpoch") {
		if a.ActualValue.PartitionLeaderEpoch != a.ExpectedValue.PartitionLeaderEpoch {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "PartitionLeaderEpoch", a.ExpectedValue.PartitionLeaderEpoch, a.ActualValue.PartitionLeaderEpoch)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ PartitionLeaderEpoch: %d", a.ActualValue.PartitionLeaderEpoch)
	}

	if !Contains(a.excludedBatchFields, "Magic") {
		if a.ActualValue.Magic != a.ExpectedValue.Magic {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "Magic", a.ExpectedValue.Magic, a.ActualValue.Magic)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ Magic: %d", a.ActualValue.Magic)
	}

	if !Contains(a.excludedBatchFields, "CRC") {
		if a.ActualValue.CRC != a.ExpectedValue.CRC {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "CRC", a.ExpectedValue.CRC, a.ActualValue.CRC)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ CRC: %d", a.ActualValue.CRC)
	}

	if !Contains(a.excludedBatchFields, "Attributes") {
		if a.ActualValue.Attributes != a.ExpectedValue.Attributes {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "Attributes", a.ExpectedValue.Attributes, a.ActualValue.Attributes)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ Attributes: %d", a.ActualValue.Attributes)
	}

	if !Contains(a.excludedBatchFields, "LastOffsetDelta") {
		if a.ActualValue.LastOffsetDelta != a.ExpectedValue.LastOffsetDelta {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "LastOffsetDelta", a.ExpectedValue.LastOffsetDelta, a.ActualValue.LastOffsetDelta)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ LastOffsetDelta: %d", a.ActualValue.LastOffsetDelta)
	}

	if !Contains(a.excludedBatchFields, "FirstTimestamp") {
		if a.ActualValue.FirstTimestamp != a.ExpectedValue.FirstTimestamp {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "FirstTimestamp", a.ExpectedValue.FirstTimestamp, a.ActualValue.FirstTimestamp)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ FirstTimestamp: %d", a.ActualValue.FirstTimestamp)
	}

	if !Contains(a.excludedBatchFields, "MaxTimestamp") {
		if a.ActualValue.MaxTimestamp != a.ExpectedValue.MaxTimestamp {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "MaxTimestamp", a.ExpectedValue.MaxTimestamp, a.ActualValue.MaxTimestamp)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ MaxTimestamp: %d", a.ActualValue.MaxTimestamp)
	}

	if !Contains(a.excludedBatchFields, "ProducerId") {
		if a.ActualValue.ProducerId != a.ExpectedValue.ProducerId {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "ProducerId", a.ExpectedValue.ProducerId, a.ActualValue.ProducerId)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ ProducerId: %d", a.ActualValue.ProducerId)
	}

	if !Contains(a.excludedBatchFields, "ProducerEpoch") {
		if a.ActualValue.ProducerEpoch != a.ExpectedValue.ProducerEpoch {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "ProducerEpoch", a.ExpectedValue.ProducerEpoch, a.ActualValue.ProducerEpoch)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ ProducerEpoch: %d", a.ActualValue.ProducerEpoch)
	}

	if !Contains(a.excludedBatchFields, "BaseSequence") {
		if a.ActualValue.BaseSequence != a.ExpectedValue.BaseSequence {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "BaseSequence", a.ExpectedValue.BaseSequence, a.ActualValue.BaseSequence)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ BaseSequence: %d", a.ActualValue.BaseSequence)
	}

	if !Contains(a.excludedBatchFields, "RecordCount") {
		actualCount := len(a.ActualValue.Records)
		expectedCount := len(a.ExpectedValue.Records)
		if actualCount != expectedCount {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "RecordCount", expectedCount, actualCount)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ RecordCount: %d", actualCount)
	}

	return a
}

// AssertOnlyBatch asserts only the RecordBatch-level fields
func (a *RecordBatchAssertion) AssertOnlyBatch() *RecordBatchAssertion {
	return a.SkipRecordFields().AssertBatch()
}

// AssertBatchAndRecords asserts both batch and record fields
func (a *RecordBatchAssertion) AssertBatchAndRecords() *RecordBatchAssertion {
	return a.AssertBatch().assertRecords()
}

// assertRecords is the internal function to validate individual records within the batch
// Fields asserted by default: Length, Attributes, TimestampDelta, OffsetDelta, Key, Value, Headers
func (a *RecordBatchAssertion) assertRecords() *RecordBatchAssertion {
	if a.err != nil {
		return a
	}

	if a.excludedRecordFields == nil {
		return a
	}

	actualRecords := a.ActualValue.Records
	expectedRecords := a.ExpectedValue.Records

	if !Contains(a.excludedRecordFields, "Length") {
		if len(actualRecords) != len(expectedRecords) {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "Records.length", len(expectedRecords), len(actualRecords))
			return a
		}
	}

	for i, actualRecord := range actualRecords {
		expectedRecord := expectedRecords[i]
		
		if !Contains(a.excludedRecordFields, "Attributes") {
			if actualRecord.Attributes != expectedRecord.Attributes {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Record[%d] Attributes", i), expectedRecord.Attributes, actualRecord.Attributes)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ Record[%d] Attributes: %d", i, actualRecord.Attributes)
		}

		if !Contains(a.excludedRecordFields, "TimestampDelta") {
			if actualRecord.TimestampDelta != expectedRecord.TimestampDelta {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Record[%d] TimestampDelta", i), expectedRecord.TimestampDelta, actualRecord.TimestampDelta)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ Record[%d] TimestampDelta: %d", i, actualRecord.TimestampDelta)
		}

		if !Contains(a.excludedRecordFields, "OffsetDelta") {
			if actualRecord.OffsetDelta != expectedRecord.OffsetDelta {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Record[%d] OffsetDelta", i), expectedRecord.OffsetDelta, actualRecord.OffsetDelta)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ Record[%d] OffsetDelta: %d", i, actualRecord.OffsetDelta)
		}

		if !Contains(a.excludedRecordFields, "Key") {
			if !reflect.DeepEqual(actualRecord.Key, expectedRecord.Key) {
				a.err = fmt.Errorf("Expected %s to be %q, got %q", fmt.Sprintf("Record[%d] Key", i), string(expectedRecord.Key), string(actualRecord.Key))
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ Record[%d] Key: %q", i, string(actualRecord.Key))
		}

		if !Contains(a.excludedRecordFields, "Value") {
			if !reflect.DeepEqual(actualRecord.Value, expectedRecord.Value) {
				a.err = fmt.Errorf("Expected %s to be %q, got %q", fmt.Sprintf("Record[%d] Value", i), string(expectedRecord.Value), string(actualRecord.Value))
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ Record[%d] Value: %q", i, string(actualRecord.Value))
		}

		if !Contains(a.excludedRecordFields, "Headers") {
			if !a.assertHeaders(actualRecord.Headers, expectedRecord.Headers, i) {
				return a
			}
		}
	}

	return a
}

// assertHeaders validates record headers
func (a *RecordBatchAssertion) assertHeaders(actualHeaders []kafkaapi.RecordHeader, expectedHeaders []kafkaapi.RecordHeader, recordIndex int) bool {
	if len(actualHeaders) != len(expectedHeaders) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Record[%d] Headers.length", recordIndex), len(expectedHeaders), len(actualHeaders))
		return false
	}

	for j, actualHeader := range actualHeaders {
		expectedHeader := expectedHeaders[j]

		if actualHeader.Key != expectedHeader.Key {
			a.err = fmt.Errorf("Expected %s to be %q, got %q", fmt.Sprintf("Record[%d] Header[%d] Key", recordIndex, j), expectedHeader.Key, actualHeader.Key)
			return false
		}

		if !reflect.DeepEqual(actualHeader.Value, expectedHeader.Value) {
			a.err = fmt.Errorf("Expected %s to be %q, got %q", fmt.Sprintf("Record[%d] Header[%d] Value", recordIndex, j), string(expectedHeader.Value), string(actualHeader.Value))
			return false
		}

		protocol.SuccessLogWithIndentation(a.logger, 3, "✓ Record[%d] Header[%d]: %s = %q", recordIndex, j, actualHeader.Key, string(actualHeader.Value))
	}

	return true
}

// AssertRecordValues is a convenience method to assert only record values as strings
func (a *RecordBatchAssertion) AssertRecordValues(expectedValues []string) *RecordBatchAssertion {
	if a.err != nil {
		return a
	}

	actualRecords := a.ActualValue.Records
	if len(actualRecords) != len(expectedValues) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "Records.length", len(expectedValues), len(actualRecords))
		return a
	}

	for i, actualRecord := range actualRecords {
		expectedValue := expectedValues[i]
		actualValue := string(actualRecord.Value)

		if actualValue != expectedValue {
			a.err = fmt.Errorf("Expected %s to be %q, got %q", fmt.Sprintf("Record[%d] Value", i), expectedValue, actualValue)
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 2, "✓ Record[%d] Value: %q", i, actualValue)
	}

	return a
}

// AssertRecordCount is a convenience method to assert only the number of records
func (a *RecordBatchAssertion) AssertRecordCount(expectedCount int) *RecordBatchAssertion {
	if a.err != nil {
		return a
	}

	actualCount := len(a.ActualValue.Records)
	if actualCount != expectedCount {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "RecordCount", expectedCount, actualCount)
		return a
	}
	protocol.SuccessLogWithIndentation(a.logger, 1, "✓ RecordCount: %d", actualCount)
	return a
}

// AssertEmptyRecords asserts that the batch has no records
func (a *RecordBatchAssertion) AssertEmptyRecords() *RecordBatchAssertion {
	if a.err != nil {
		return a
	}

	actualCount := len(a.ActualValue.Records)
	if actualCount != 0 {
		a.err = fmt.Errorf("Expected batch to have empty records, got %d records", actualCount)
		return a
	}
	protocol.SuccessLogWithIndentation(a.logger, 1, "✓ Batch has empty records")
	return a
}

// Run executes the assertion and returns any error
func (a *RecordBatchAssertion) Run() error {
	return a.err
}