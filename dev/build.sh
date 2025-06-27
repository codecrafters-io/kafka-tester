#!/bin/bash

# Build script for Kafka development tools
# Usage: ./dev/build.sh

set -e

echo "Building Kafka development tools..."

# Create dist directory if it doesn't exist
mkdir -p dist

# Build all the development tools
echo "Building topic data decoder..."
go build -o dist/decode-topic ./dev/decode-topic

echo "Building index decoder..."
go build -o dist/decode-index ./dev/decode-index

echo "Building time index decoder..."
go build -o dist/decode-timeindex ./dev/decode-timeindex

echo "Building snapshot decoder..."
go build -o dist/decode-snapshot ./dev/decode-snapshot

echo "Building bootstrap checkpoint decoder..."
go build -o dist/decode-bootstrap ./dev/decode-bootstrap

echo "Building Kafka logs explorer..."
go build -o dist/kafka-explorer ./dev/kafka-explorer

echo ""
echo "✅ All tools built successfully!"
echo ""
echo "Available tools:"
echo "  dist/decode-topic      - Parse .log files (topic data)"
echo "  dist/decode-index      - Parse .index files (offset index)"
echo "  dist/decode-timeindex  - Parse .timeindex files (time index)"
echo "  dist/decode-snapshot   - Parse .snapshot files"
echo "  dist/decode-bootstrap  - Parse bootstrap.checkpoint file"
echo "  dist/kafka-explorer    - Comprehensive recursive logs explorer"
echo ""
echo "Example usage:"
echo "  ./dist/kafka-explorer /tmp/kraft-combined-logs"
echo "  ./dist/decode-topic /tmp/kraft-combined-logs/quickstart-events-0/00000000000000000000.log"