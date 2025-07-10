package assertions

import "slices"

// Contains checks if a target string exists within the slice.
func Contains(slice []string, target string) bool {
	return slices.Contains(slice, target)
}
