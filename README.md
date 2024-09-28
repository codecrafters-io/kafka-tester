# Kafka Challenge Tester

This is a program that validates your progress on the "Build your own kafka" challenge.

## Requirements for binary

- Following environment variables:
  - `CODECRAFTERS_REPOSITORY_DIR` - root of the user's code submission
  - `CODECRAFTERS_TEST_CASES_JSON` - test cases in JSON format

## User code requirements

- A binary named `your_program.sh` that executes the program.
- A file named `codecrafters.yml`, with the following values:
  - `debug`

# Kafka Protocol Grammar

```
RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32
ErrorCode = 0
APIKey: ApiVersions =	18 
```
```
Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER 
  request_api_key => INT16
  request_api_version => INT16
  correlation_id => INT32
  client_id => NULLABLE_STRING
```
```
Response Header v1 => correlation_id TAG_BUFFER 
  correlation_id => INT32
```

```
ApiVersions Request (Version: 2) => 
```

```
ApiVersions Request (Version: 3) => client_software_name client_software_version TAG_BUFFER 
  client_software_name => COMPACT_STRING
  client_software_version => COMPACT_STRING
```

```
ApiVersions Response (Version: 2) => error_code [api_keys] throttle_time_ms 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
```

```
Fetch Response (Version: 10) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
```

```
Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset TAG_BUFFER 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
```

```
ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms TAG_BUFFER 
  error_code => INT16
  api_keys => api_key min_version max_version TAG_BUFFER 
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
```

```
ApiVersions Response (Version: 0) => error_code [api_keys] 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
``` 

# Running Kafka locally
```
rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
```

DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor TAG_BUFFER 
  topics => name TAG_BUFFER 
    name => COMPACT_STRING
  response_partition_limit => INT32
  cursor => topic_name partition_index TAG_BUFFER 
    topic_name => COMPACT_STRING
    partition_index => INT32
  
cursor field is nullable.
if it is null, it will be replaced with a `ff`