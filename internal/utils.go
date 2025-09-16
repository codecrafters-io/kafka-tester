package internal

import (
	"fmt"
	"math"

	"github.com/codecrafters-io/tester-utils/random"
)

func getInvalidAPIVersion() int16 {
	return int16(random.RandomInt(1000, 2000))
}

func getRandomCorrelationId() int32 {
	return int32(random.RandomInt(0, math.MaxInt32-1))
}

func getRandomTopicUUID() string {
	return fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random.RandomInt(1, 100))
}

func getRandomTopicNames(count int) []string {
	return random.RandomWords(count)
}

func getRandomTopicUUIDs(count int) []string {
	randomInts := random.RandomInts(1, 100, count)
	uuids := []string{}

	for _, randomInt := range randomInts {
		uuids = append(uuids, fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", randomInt))
	}

	return uuids
}

func getEmptyTopicUUID() string {
	return "00000000-0000-0000-0000-000000000000"
}
