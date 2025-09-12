package internal

import (
	"fmt"
	"math"
	"slices"

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

// getRandomTopicNames returns sorted random words slice
// We sort these values because kafka returns topics in alphabetically sorted way
// eg bar -> baz -> foo
// This doesn't feel right
// This can be in either of three places
// 1. Here
// 2. Inside generate function of kafka_files_generator where we sort the values and generate directories
// 3. Inside assertion
// Previously, the topics were created in sorted manner so no sorting was needed in assertion
// This information should have been conveyed using stage instructions. Don't know whether changing instructions would be a good idea.
func getRandomTopicNames(count int) []string {
	topicNames := random.RandomWords(count)
	slices.Sort(topicNames)
	return topicNames
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
