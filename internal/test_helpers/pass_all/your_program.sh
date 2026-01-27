#!/bin/sh
# echo "Starting Kafka using KRaft (Kafka Raft Metadata)"
# For testing locally, you need to have the latest kafka version in /usr/local/bin/kafka-latest
# Get it from here: https://github.com/codecrafters-io/build-your-own-kafka/blob/main/kafka_2.13-4.0.0-SNAPSHOT.tgz
# Extract it to /usr/local as kafka-latest and make sure you update the permissions
# tar -xvf kafka_2.13-4.0.0-SNAPSHOT.tgz
# mv kafka_2.13-4.0.0-SNAPSHOT.tgz kafka-latest
# mv kafka-latest /usr/local
# sudo chown -R user:group ./kafka-latest
# rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
SCRIPT_DIR="$(dirname "$0")"
/usr/local/kafka-latest/bin/kafka-server-start.sh $@ > /dev/null