package internal

import (
	"math"

	"github.com/codecrafters-io/tester-utils/random"
)

func getInvalidAPIVersion() int {
	apiVersion := 1
	for apiVersion <= 3 && apiVersion >= 0 {
		apiVersion = random.RandomInt(0, math.MaxInt16)
	}
	return random.RandomElementFromArray([]int{apiVersion, -apiVersion})
}

func getRandomCorrelationId() int32 {
	return int32(random.RandomInt(0, math.MaxInt32-1))
}
