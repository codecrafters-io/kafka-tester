#!/bin/sh
# echo "Starting Kafka using KRaft (Kafka Raft Metadata)"
# For testing locally, you need to have the latest kafka version in /usr/local/bin/kafka-latest
# Get it from here: https://github.com/codecrafters-io/build-your-own-kafka/blob/main/kafka_2.13-4.0.0-SNAPSHOT.tgz
# Extract it and make sure you update the permissions
# sudo chown -R user:group ./kafka-latest
filePath="/tmp/test-$(date +%s%3N)"
echo "FilePath: $filePath"
echo "Started: $(date)" >"$filePath"

# Trap EXIT signal to ensure cleanup
trap 'cleanup' EXIT

cleanup() {
    echo "Exit trapped"
    echo "Cleanup finished: $(date)" >>"$filePath"
}

/usr/local/kafka-latest/bin/kafka-server-start.sh "$@" >>"$filePath" 2>&1
echo "Finished: $(date)" >>"$filePath"
