package assertions

import (
	"regexp"

	"github.com/codecrafters-io/kafka-tester/internal/assertions/validations"
)

type ApiVersionsResponseAssertion struct {
	Validations validations.ValidationMap
}

func NewApiVersionsResponseAssertion() *ApiVersionsResponseAssertion {
	return &ApiVersionsResponseAssertion{
		Validations: make(validations.ValidationMap),
	}
}

func (a *ApiVersionsResponseAssertion) GetPrimitiveValidation(locator string) validations.Validation {
	validation, ok := a.Validations[locator]

	// Get matched validator
	if ok {
		return validation
	}

	// Or look for a pattern that the locator complies with
	for k, v := range a.Validations {
		matched, _ := regexp.MatchString(k, locator)
		if matched {
			return v
		}
	}

	return nil
}

func (a *ApiVersionsResponseAssertion) SetPrimitiveValidations(validations validations.ValidationMap) *ApiVersionsResponseAssertion {
	a.Validations = validations
	return a
}
