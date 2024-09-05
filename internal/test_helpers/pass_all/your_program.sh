#!/bin/sh
# echo "Starting Kafka using KRaft (Kafka Raft Metadata)"

SCRIPT_DIR=$(dirname "$(realpath "$0")")

/usr/local/kafka/bin/kafka-server-start.sh $SCRIPT_DIR/kraft.server.properties --override log.dirs=$SCRIPT_DIR/kraft-combined-logs > /dev/null 2>&1
