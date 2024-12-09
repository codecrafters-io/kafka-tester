package common

import (
	"fmt"
	"slices"
	"sort"

	"github.com/codecrafters-io/tester-utils/random"
)

const (
	LOG_DIR                     = "/tmp/kraft-combined-logs"
	SERVER_PROPERTIES_FILE_PATH = "/tmp/server.properties"

	CLUSTER_ID = "IAAAAAAAQACAAAAAAAAAAQ" // UUID: "20000000-0000-4000-8000-000000000001"
	NODE_ID    = 1
	VERSION    = 1

	CLUSTER_METADATA_TOPIC_ID = "AAAAAAAAAAAAAAAAAAAAAQ" // UUID: "00000000-0000-0000-0000-000000000001"
	DIRECTORY_UUID            = "10000000-0000-4000-8000-000000000001"
)

var (
	all_topic_names    = []string{"foo", "bar", "baz", "qux", "quz", "pax", "paz", "saz"}
	topic_names        = GetSortedValues(random.RandomElementsFromArray(all_topic_names, 4))
	TOPIC1_NAME        = topic_names[0]
	TOPIC2_NAME        = topic_names[1]
	TOPIC3_NAME        = topic_names[2]
	random_topic_uuids = getUniqueRandomIntegers(10, 99, 4)
	TOPIC1_UUID        = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random_topic_uuids[1])
	TOPIC2_UUID        = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random_topic_uuids[2])
	TOPIC3_UUID        = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random_topic_uuids[3])
	TOPICX_UUID        = fmt.Sprintf("00000000-0000-0000-0000-00000000%04d", random.RandomInt(1000, 9999)) // Unknown topic used in requests

	TOPIC_UNKOWN_NAME = fmt.Sprintf("unknown-topic-%s", topic_names[3])
	TOPIC_UNKOWN_UUID = "00000000-0000-0000-0000-000000000000"

	all_messages = []string{"Hello World!", "Hello Earth!", "Hello Reverse Engineering!", "Hello Universe!", "Hello Kafka!", "Hello CodeCrafters!"}
	messages     = random.RandomElementsFromArray(all_messages, 3)
	MESSAGE1     = messages[0]
	MESSAGE2     = messages[1]
	MESSAGE3     = messages[2]
)

func GetSortedValues[T string](values []T) []T {
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	return values
}

func getUniqueRandomIntegers(min, max, count int) []int {
	randomInts := []int{}
	for i := 0; i < count; i++ {
		randomInt := random.RandomInt(min, max)
		for slices.Contains(randomInts, randomInt) {
			randomInt = random.RandomInt(min, max)
		}
		randomInts = append(randomInts, randomInt)
	}
	return randomInts
}
