package internal

import (
	"encoding/base64"
	"math"
	"slices"
	"strings"

	"github.com/codecrafters-io/tester-utils/random"
	"github.com/google/uuid"
)

func getInvalidAPIVersion() int16 {
	return int16(random.RandomInt(1000, 2000))
}

func getRandomCorrelationId() int32 {
	return int32(random.RandomInt(0, math.MaxInt32-1))
}

func getRandomTopicUUID() string {
	// whenever these are encountered, kafka crashes due to illegal character exception
	// so, we use url-safe base64
	urlUnsafeCharacters := []string{"+", "-", "/", "_"}

	for {
		id := uuid.New()
		base54Id := base64.StdEncoding.EncodeToString(id[:])
		isURLSafe := true

		for _, char := range urlUnsafeCharacters {
			if strings.Contains(base54Id, char) {
				isURLSafe = false
				break
			}
		}

		if isURLSafe {
			return id.String()
		}
	}
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

	for range randomInts {
		uuids = append(uuids, uuid.NewString())
	}

	return uuids
}

func getEmptyTopicUUID() string {
	return "00000000-0000-0000-0000-000000000000"
}
