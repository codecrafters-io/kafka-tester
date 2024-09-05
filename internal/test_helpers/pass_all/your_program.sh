#!/bin/sh
echo "Starting Kafka using KRaft (Kafka Raft Metadata)"
# For fresh start
# KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
# /kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
/usr/local/kafka/bin/kafka-server-start.sh ./internal/test_helpers/pass_all/kraft.server.properties --override log.dirs=./internal/test_helpers/pass_all/kraft-combined-logs > /dev/null 2>&1
