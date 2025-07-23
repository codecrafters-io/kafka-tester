package assertions

import "fmt"

// Contains checks if a target string exists within the slice.
func Contains(slice []string, target string) bool {
	for _, item := range slice {
		if item == target {
			return true
		}
	}
	return false
}

// validateExclusions validates that the excluded fields are valid for the excludable fields
// structure: response position (Body, TopicResponse, etc.) -> excludable fields, excluded fields
func validateExclusions(exclusionMap map[string]struct {
	excludedFields   []string
	excludableFields []string
}) error {

	for name, data := range exclusionMap {
		for _, field := range data.excludedFields {
			if !Contains(data.excludableFields, field) {
				return fmt.Errorf("CodeCrafters Internal Error: Invalid exclusion for %s: %s", name, field)
			}
		}
	}

	return nil
}
