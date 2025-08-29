package assertions_legacy

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

// mustValidateExclusions validates that the excluded fields are valid for the excludable fields
func mustValidateExclusions(excludedFields []string, excludableFields []string) {
	for _, field := range excludedFields {
		if !Contains(excludableFields, field) {
			panic(fmt.Sprintf("CodeCrafters Internal Error: Exclusion %s is not supported", field))
		}
	}
}
