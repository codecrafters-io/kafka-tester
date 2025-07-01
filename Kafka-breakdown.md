Kafka / Produce API

# Stage 1: Include Produce in APIVersions

In this stage, you'll add an entry for the `Produce` API to the APIVersions response.

## APIVersions

Your Kafka implementation should include the Produce API (key=0) in the ApiVersions response before implementing produce functionality. This would let the client know that the broker supports the Produce API.

## Tests

The tester will execute your program like this:

```bash
./your_program.sh
```

It'll then connect to your server on port 9092 and send a valid `APIVersions` (v4) request.

The tester will validate that:

- The first 4 bytes of your response (the "message length") are valid.
- The correlation ID in the response header matches the correlation ID in the request header.
- The error code in the response body is `0` (No Error).
- The response body contains at least one entry for the API key `0` (Produce).
- The `MaxVersion` for the Produce API is at least 11.

## Notes

- You don't have to implement support for the `Produce` request in this stage. We'll get to this in later stages.
- You'll still need to include the entry for `APIVersions` in your response to pass the previous stage.

# Stage 2: Produce to Unknown Topic

In this stage, you'll add support for handling Produce requests to non-existent topics with proper error responses.

## Handling Non-Existent Topics

Your Kafka implementation should validate topic existence and return appropriate error codes when a client tries to produce to a non-existent topic. Kafka stores metadata about topics in the `__cluster_metadata` topic. To check if a topic exists or not, you'll need to read the `__cluster_metadata` topic's log file, located at `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`. If the topic exists, the topic will have a directory with all the data required for it to operate at `<log-dir>/<topic-name>-<partition-index>`.
TODO: Do we need to explain the TOPIC_RECORD record in the `__cluster_metadata` log.

## Tests

The tester will execute your program like this:

```bash
./your_program.sh
```

It'll then connect to your server on port 9092 and send a `Produce` (v11) request to a non-existent topic.

The tester will validate that:

- The first 4 bytes of your response (the "message length") are valid.
- The correlation ID in the response header matches the correlation ID in the request header.
- The error code in the response body is `3` (UNKNOWN_TOPIC_OR_PARTITION).
- The `throttle_time_ms` field in the response is `0`.
- The `name` field in the topic response inside response should correspond to the topic name in the request.
- The `partition` field in the partition response inside topic response should correspond to the partition in the request.
- The `offset` field in the partition response inside topic response should be `-1`.
- The `timestamp` field in the partition response inside topic response should be `-1`.
- The `log start offset` field in the partition response inside topic response should be `-1`.

## Notes

- You'll need to parse the `Produce` request in this stage to get the topic name and partition to send in the response.
- The official docs for the `Produce` request can be found [here](https://kafka.apache.org/protocol.html#The_Messages_Produce). Make sure to scroll down to the "Produce Response (Version: 11)" section.
- The official Kafka docs don't cover the structure of records inside the `__cluster_metadata` topic, but you can find the definitions in the Kafka source code [here](https://github.com/apache/kafka/tree/5b3027dfcbcb62d169d4b4421260226e620459af/metadata/src/main/resources/common/metadata).

# Stage 3: Produce to Invalid Partition

In this stage, you'll add support for handling Produce requests to invalid partitions for known topics with proper error responses.

## Handling Invalid Partitions

Your Kafka implementation should validate that partition indices exist for known topics and return appropriate errors for invalid partitions. Kafka stores metadata about partitions in the `__cluster_metadata` topic. To check if a partition exists or not, you'll need to read the `__cluster_metadata` topic's log file, located at `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`. If the partition exists, the partition will have a directory with all the data required for it to operate at `<log-dir>/<topic-name>-<partition-index>`.
TODO: Do we need to explain the PARTITION_RECORD record in the `__cluster_metadata` log.

## Tests

The tester will execute your program like this:

```bash
./your_program.sh
```

It'll then connect to your server on port 9092 and send a `Produce` (v11) request to a known topic with an invalid partition.

The tester will validate that:

- The first 4 bytes of your response (the "message length") are valid.
- The correlation ID in the response header matches the correlation ID in the request header.
- The error code in the response body is `3` (UNKNOWN_TOPIC_OR_PARTITION).
- The `throttle_time_ms` field in the response is `0`.
- The `name` field in the topic response inside response should correspond to the topic name in the request.
- The `partition` field in the partition response inside topic response should correspond to the partition in the request.
- The `offset` field in the partition response inside topic response should be `-1`.
- The `timestamp` field in the partition response inside topic response should be `-1`.
- The `log start offset` field in the partition response inside topic response should be `-1`.

## Notes

- The official docs for the `Produce` request can be found [here](https://kafka.apache.org/protocol.html#The_Messages_Produce). Make sure to scroll down to the "Produce Response (Version: 11)" section.
- The official Kafka docs don't cover the structure of records inside the `__cluster_metadata` topic, but you can find the definitions in the Kafka source code [here](https://github.com/apache/kafka/tree/5b3027dfcbcb62d169d4b4421260226e620459af/metadata/src/main/resources/common/metadata).

# Stage 4: Single Record Produce

In this stage, you'll add support for successfully producing a single record to a valid topic and partition.

## Single Record Production

Your Kafka implementation should accept valid Produce requests, store the record in the appropriate log file, and return successful responses with assigned offsets.

## Tests

The tester will create topics and send valid Produce requests:

```bash
# Setup - create topic with partitions
./your_program.sh
# Topic "test-topic" created with partitions 0, 1, 2
```

It will then send a successful Produce request and verify the response and data persistence.

```bash
# This request should succeed
Produce Request (API Key: 0, Version: 11)
- Topic: "test-topic"
- Partition: 0
- Record: "Hello Kafka!"
- Acks: 1 (leader acknowledgment)
- Timeout: 5000ms

# Expected response:
Produce Response with:
- Topic: "test-topic"
- Partition: 0
- Error Code: 0 (NO_ERROR)
- Offset: 0 (first message)
- Timestamp: <current_time>
```

## Notes

- Error code 0 (success) required
- Offset assigned (typically 0 for first message)
- Record must be persisted to log file
- Foundation for all produce operations

# Stage 5: Multi-Record Batch Produce

In this stage, you'll add support for producing multiple records within a single RecordBatch.

## Batch Processing

Your Kafka implementation should handle multiple records in a single batch, assign sequential offsets, and validate the LastOffsetDelta field correctly.

## Tests

The tester will send Produce requests with multiple records in a single batch:

```bash
# Setup - same as PR4
./your_program.sh
# Topic "test-topic" created
```

It will then send multi-record batches and verify all records are stored with sequential offsets.

```bash
# This request contains multiple records in one batch
Produce Request (API Key: 0, Version: 11)
- Topic: "test-topic"
- Partition: 0
- RecordBatch containing 3 records:
  * Record 1: "Message 1"
  * Record 2: "Message 2"
  * Record 3: "Message 3"
- LastOffsetDelta: 2 (num_records - 1)
- Acks: 1

# Expected response:
Produce Response with:
- Topic: "test-topic"
- Partition: 0
- Error Code: 0 (NO_ERROR)
- Base Offset: <assigned_offset>
```

## Notes

- All records must be stored successfully
- Sequential offset assignment (e.g., 0, 1, 2)
- Single response for the entire batch
- Correct LastOffsetDelta handling required

# Stage 6: Multi-Partition Produce

In this stage, you'll add support for producing to multiple partitions of the same topic in a single request.

## Partition Routing

Your Kafka implementation should handle writes to multiple partitions within the same topic, manage independent offset assignment per partition, and aggregate responses correctly.

## Tests

The tester will send Produce requests targeting multiple partitions:

```bash
# Setup - topic with multiple partitions
./your_program.sh
# Topic "test-topic" created with partitions 0, 1, 2
```

It will then send requests to multiple partitions and verify independent handling.

```bash
# This request targets multiple partitions
Produce Request (API Key: 0, Version: 11)
- Topic: "test-topic"
- Partitions:
  * Partition 0: "Message for partition 0"
  * Partition 1: "Message for partition 1"
  * Partition 2: "Message for partition 2"
- Acks: 1

# Expected response:
Produce Response with:
- Topic: "test-topic"
- Partition responses:
  * Partition 0: Error=0, Offset=<assigned>
  * Partition 1: Error=0, Offset=<assigned>
  * Partition 2: Error=0, Offset=<assigned>
```

## Notes

- All partitions must receive data successfully
- Independent offset assignment per partition
- Separate error handling per partition
- Response includes all requested partitions

# Stage 7: Multi-Topic Produce

In this stage, you'll add support for producing to multiple topics in a single request.

## Cross-Topic Production

Your Kafka implementation should handle complex requests with multiple topics, manage independent offset tracking per topic, and handle complex response structures.

## Tests

The tester will create multiple topics and send cross-topic requests:

```bash
# Setup - multiple topics
./your_program.sh
# Topics created: "topic1", "topic2", "topic3"
```

It will then send requests spanning multiple topics and verify proper handling.

```bash
# This request spans multiple topics
Produce Request (API Key: 0, Version: 11)
- Topics:
  * "topic1" → Partition 0: "Message for topic1"
  * "topic2" → Partition 0: "Message for topic2"
  * "topic3" → Partition 0: "Message for topic3"
- Acks: 1

# Expected response:
Produce Response with:
- Topic responses:
  * "topic1": Partition 0: Error=0, Offset=<assigned>
  * "topic2": Partition 0: Error=0, Offset=<assigned>
  * "topic3": Partition 0: Error=0, Offset=<assigned>
```

## Notes

- All topics must receive data successfully
- Independent offset tracking per topic
- Proper topic-level error handling
- Complex response structure handling required

---

## Setup Commands

```bash
# Clean environment
rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs

# Generate cluster ID and format storage
KAFKA_CLUSTER_ID="$(/usr/local/kafka-latest/bin/kafka-storage.sh random-uuid)"
/usr/local/kafka-latest/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c /usr/local/kafka-latest/config/kraft/server.properties

# Start Kafka server
/usr/local/kafka-latest/bin/kafka-server-start.sh /usr/local/kafka-latest/config/kraft/server.properties

# Create test topics (as needed per stage)
/usr/local/kafka-latest/bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 3
```

## Testing Commands

```bash
# Build tester
make build

# Run individual test
go run main.go

# Verify with Kafka CLI
/usr/local/kafka-latest/bin/kafka-console-consumer.sh --topic test-topic --bootstrap-server localhost:9092 --from-beginning
```

## Error Code Reference

| Code | Name | Stages |
|------|------|---------|
| 0 | NO_ERROR | 4, 5, 6, 7 |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | 2, 3 |
| 87 | RECORD_BATCH_TOO_LARGE | Debug cases |