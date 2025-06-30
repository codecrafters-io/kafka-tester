# Proposed Produce Request Stages

Stage PR1: API Version with Produce Key
- Verify that the ApiVersions response includes the Produce API (key=0) with appropriate version support
- Similar to how F1 and P1 verify API support before testing functionality

Stage PR2: Produce with Invalid Topic
- Send a Produce request to a non-existent topic
- Expect appropriate error response (UNKNOWN_TOPIC_OR_PARTITION)
- Tests error handling before valid cases

Stage PR3: Produce with Invalid Partition
- Send a Produce request to a known topic but wrong partition index
- For example: topic exists with only partition 0, but request partition 5
- Expect error response: UNKNOWN_TOPIC_OR_PARTITION (error code 3)
- Tests partition validation logic

Stage PR4: Basic Single Record Produce
- Produce a single record to a known topic/partition
- Verify successful response with proper partition metadata
- Foundation for all produce operations

Stage PR5: Multi-Record Batch Produce
- Produce multiple records in a single batch to one partition
- Tests batching logic and record processing

Stage PR6: Multi-Partition Produce
- Produce records to multiple partitions of the same topic
- Tests partition-aware routing and response handling

Stage PR7: Multi-Topic Produce
- Produce records to multiple topics in a single request
- Tests complex request/response structure with multiple topics


# Detailed Implementation Plan for Produce Stages

Stage PR1: API Version with Produce Key

func testStagePR1(stageHarness *test_case_harness.TestCaseHarness) error {
    // Standard setup pattern
    b := kafka_executable.NewKafkaExecutable(stageHarness)

    // No log generation needed - just testing API versions
    if err := b.Run(); err != nil {
        return err
    }

    broker := protocol.NewBroker("localhost:9092")
    if err := broker.ConnectWithRetries(b, logger); err != nil {
        return err
    }
    defer broker.Close()

    // Send ApiVersions request (copy from existing stages)
    correlationId := getRandomCorrelationId()
    request := kafkaapi.ApiVersionsRequest{
        Header: kafkaapi.RequestHeader{
            ApiKey:        18, // ApiVersions
            ApiVersion:    4,
            CorrelationId: correlationId,
            ClientId:      "kafka-cli",
        },
        Body: kafkaapi.ApiVersionsRequestBody{
            ClientSoftwareName:    "kafka-cli",
            ClientSoftwareVersion: "0.1",
        },
    }

    // Send and validate response includes Produce API (key=0)
    // Check MinVersion <= some_version >= 0 and MaxVersion >= some_version
}

Stage PR2: Produce with Unknown Topic

func testStagePR2(stageHarness *test_case_harness.TestCaseHarness) error {
    // Standard setup + empty log generation
    err := serializer.GenerateLogDirs(logger, false) // false = no data

    // Build Produce request to unknown topic
    correlationId := getRandomCorrelationId()
    request := kafkaapi.ProduceRequest{
        Header: kafkaapi.RequestHeader{
            ApiKey:        0, // Produce
            ApiVersion:    9, // Use version 9 like other implementations
            CorrelationId: correlationId,
            ClientId:      "kafka-tester",
        },
        Body: kafkaapi.ProduceRequestBody{
            TransactionId: nil,
            Acks:          1, // Wait for leader acknowledgment
            TimeoutMs:     5000,
            TopicData: []kafkaapi.TopicProduceData{{
                Name: "unknown-topic-" + randomString(), // Generate random unknown topic
                PartitionData: []kafkaapi.PartitionProduceData{{
                    Index: 0,
                    Records: createSingleRecordBatch("Hello World!"),
                }},
            }},
        },
    }

    // Validate response has error code 3 (UNKNOWN_TOPIC_OR_PARTITION)
}

Stage PR3: Basic Single Record Produce

func testStagePR3(stageHarness *test_case_harness.TestCaseHarness) error {
    // Generate log dirs WITH data (like F5/F6)
    err := serializer.GenerateLogDirs(logger, true)

    // Send single message to known topic
    request := kafkaapi.ProduceRequest{
        // ... header setup ...
        Body: kafkaapi.ProduceRequestBody{
            Acks:      1,
            TimeoutMs: 5000,
            TopicData: []kafkaapi.TopicProduceData{{
                Name: constants.TOPIC1_NAME, // Use existing topic from test data
                PartitionData: []kafkaapi.PartitionProduceData{{
                    Index: 0,
                    Records: createSingleRecordBatch(constants.MESSAGE1),
                }},
            }},
        },
    }

    // Validate successful response:
    // - Error code: 0 (NO_ERROR)
    // - Partition offset assigned
    // - Proper topic/partition in response
}

Stage PR4: Multi-Record Batch Produce

func testStagePR4(stageHarness *test_case_harness.TestCaseHarness) error {
    // Send multiple records in single batch to one partition
    records := createMultiRecordBatch([]string{
        constants.MESSAGE1,
        constants.MESSAGE2,
        constants.MESSAGE3,
    })

    request := kafkaapi.ProduceRequest{
        // ... similar setup ...
        Body: kafkaapi.ProduceRequestBody{
            TopicData: []kafkaapi.TopicProduceData{{
                Name: constants.TOPIC1_NAME,
                PartitionData: []kafkaapi.PartitionProduceData{{
                    Index: 0,
                    Records: records, // Multiple records in one batch
                }},
            }},
        },
    }

    // Validate all records accepted with sequential offsets
}

Stage PR5: Multi-Partition Produce

func testStagePR5(stageHarness *test_case_harness.TestCaseHarness) error {
    // Send to multiple partitions of same topic
    request := kafkaapi.ProduceRequest{
        Body: kafkaapi.ProduceRequestBody{
            TopicData: []kafkaapi.TopicProduceData{{
                Name: constants.TOPIC2_NAME, // Topic with multiple partitions
                PartitionData: []kafkaapi.PartitionProduceData{
                    {
                        Index: 0,
                        Records: createSingleRecordBatch(constants.MESSAGE1),
                    },
                    {
                        Index: 1,
                        Records: createSingleRecordBatch(constants.MESSAGE2),
                    },
                },
            }},
        },
    }

    // Validate response for both partitions
}

Stage PR6: Multi-Topic Produce

func testStagePR6(stageHarness *test_case_harness.TestCaseHarness) error {
    // Send to multiple topics (like P5 pattern)
    request := kafkaapi.ProduceRequest{
        Body: kafkaapi.ProduceRequestBody{
            TopicData: []kafkaapi.TopicProduceData{
                {
                    Name: constants.TOPIC1_NAME,
                    PartitionData: []kafkaapi.PartitionProduceData{{
                        Index: 0,
                        Records: createSingleRecordBatch(constants.MESSAGE1),
                    }},
                },
                {
                    Name: constants.TOPIC2_NAME,
                    PartitionData: []kafkaapi.PartitionProduceData{{
                        Index: 0,
                        Records: createSingleRecordBatch(constants.MESSAGE2),
                    }},
                },
                {
                    Name: constants.TOPIC3_NAME,
                    PartitionData: []kafkaapi.PartitionProduceData{{
                        Index: 0,
                        Records: createSingleRecordBatch(constants.MESSAGE3),
                    }},
                },
            },
        },
    }

    // Validate complex response with multiple topics
}


// Setup
rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
KAFKA_CLUSTER_ID="$(/usr/local/kafka-latest/bin/kafka-storage.sh random-uuid)"
/usr/local/kafka-latest/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c /usr/local/kafka-latest/config/kraft/server.properties
/usr/local/kafka-latest/bin/kafka-server-start.sh /usr/local/kafka-latest/config/kraft/server.properties
/usr/local/kafka-latest/bin/kafka-topics.sh --create --topic ryanproduce --bootstrap-server localhost:9092 --partitions 3


./dist/kafka-explorer /tmp/kraft-combined-logs/


// Test
go run main.go
./dist/kafka-cli --broker localhost:9092 --topic ryanproduce --produce --message "Hello World!"