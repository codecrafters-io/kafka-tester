#!/bin/sh
# echo "Starting Kafka using KRaft (Kafka Raft Metadata)"
# For testing locally, you need to have the latest kafka version in /usr/local/bin/kafka-latest
# Get it from here: https://github.com/codecrafters-io/build-your-own-kafka/blob/main/kafka_2.13-4.0.0-SNAPSHOT.tgz
# Extract it and make sure you update the permissions
# sudo chown -R user:group ./kafka-latest
shmPath="/dev/shm/test-$(date +%s%3N)"

if lsof -i :9092 >/dev/null 2>&1; then
    echo "bound"
    echo "bound" >>"$shmPath"
else
    echo "not bound"
    echo "not bound" >>"$shmPath"
fi

echo "ShmPath: $shmPath"
echo "Started: $(date)" >"$shmPath"

# Trap EXIT signal to ensure cleanup
trap 'cleanup' EXIT

cleanup() {
    echo "Exit trapped"
    echo "Cleanup finished: $(date)" >>"$shmPath"
    rm -f "$shmPath"
}

/usr/local/kafka-latest/bin/kafka-server-start.sh "$@" >>"$shmPath" 2>&1
echo "Finished: $(date)" >>"$shmPath"
