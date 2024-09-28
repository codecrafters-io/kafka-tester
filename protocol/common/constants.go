package common

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/tester-utils/random"
)

const (
	LOG_DIR = "/tmp/kraft-combined-logs"

	CLUSTER_ID = "IAAAAAAAQACAAAAAAAAAAQ" // UUID: "20000000-0000-4000-8000-000000000001"
	NODE_ID    = 1
	VERSION    = 1

	CLUSTER_METADATA_TOPIC_ID = "AAAAAAAAAAAAAAAAAAAAAQ" // UUID: "00000000-0000-0000-0000-000000000001"
	DIRECTORY_UUID            = "10000000-0000-4000-8000-000000000001"
)

var (
	all_topic_names = []string{"foo", "bar", "baz", "qux", "quz", "pax", "paz", "saz"}
	topic_names     = protocol.GetSortedValues(random.RandomElementsFromArray(all_topic_names, 3))
	TOPIC1_NAME     = topic_names[0]
	TOPIC2_NAME     = topic_names[1]
	TOPIC3_NAME     = topic_names[2]
	TOPIC1_UUID     = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random.RandomInt(10, 99))
	TOPIC2_UUID     = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random.RandomInt(10, 99))
	TOPIC3_UUID     = fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", random.RandomInt(10, 99))
	TOPICX_UUID     = fmt.Sprintf("00000000-0000-0000-0000-00000000%04d", random.RandomInt(1000, 9999)) // Unknown topic

	all_messages = []string{"Hello World!", "Hello Earth!", "Hello Reverse Engineering!", "Hello Universe!", "Hello Kafka!", "Hello CodeCrafters!"}
	messages     = random.RandomElementsFromArray(all_messages, 3)
	MESSAGE1     = messages[0]
	MESSAGE2     = messages[1]
	MESSAGE3     = messages[2]
)
