package common

import (
	"fmt"
	"slices"

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
	// Topic names and UUIDs - initialized in init()
	TOPIC1_NAME, TOPIC2_NAME, TOPIC3_NAME, TOPIC4_NAME string
	TOPIC1_UUID, TOPIC2_UUID, TOPIC3_UUID, TOPIC4_UUID string
	TOPICX_UUID                                        string
	TOPIC_UNKOWN_NAME, TOPIC_UNKOWN_UUID               string

	MESSAGE1, MESSAGE2, MESSAGE3 string
)

func init() {
	random.Init()

	all_topic_names := []string{"foo", "bar", "baz", "qux", "quz", "pax", "paz", "saz"}
	topic_names := getSortedValues(random.RandomElementsFromArray(all_topic_names, 4))
	TOPIC1_NAME = topic_names[0]
	TOPIC2_NAME = topic_names[1]
	TOPIC3_NAME = topic_names[2]
	TOPIC4_NAME = topic_names[3]
	random_topic_uuids := random.RandomInts(10, 100, 4)
	TOPIC1_UUID = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random_topic_uuids[0])
	TOPIC2_UUID = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random_topic_uuids[1])
	TOPIC3_UUID = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random_topic_uuids[2])
	TOPIC4_UUID = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random_topic_uuids[3])
	TOPICX_UUID = fmt.Sprintf("00000000-0000-0000-0000-00000000%04d", random.RandomInt(1000, 9999)) // Unknown topic used in requests

	TOPIC_UNKOWN_NAME = fmt.Sprintf("unknown-topic-%s", topic_names[3])
	TOPIC_UNKOWN_UUID = "00000000-0000-0000-0000-000000000000"

	all_messages := []string{"Hello World!", "Hello Earth!", "Hello Reverse Engineering!", "Hello Universe!", "Hello Kafka!", "Hello CodeCrafters!"}
	messages := random.RandomElementsFromArray(all_messages, 3)
	MESSAGE1 = messages[0]
	MESSAGE2 = messages[1]
	MESSAGE3 = messages[2]
}

func getSortedValues[T string](values []T) []T {
	slices.Sort(values)
	return values
}
