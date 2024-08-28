#!/bin/sh
echo "Starting Kafka using KRaft (Kafka Raft Metadata)"
cd /tmp
rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
cd /Users/ryang/Developer/tmp/kafka/kafka_2.13-3.8.0
KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
./bin/kafka-server-start.sh config/kraft/server.properties