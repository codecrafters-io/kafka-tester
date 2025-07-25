#!/bin/bash

set -e

# Ensure we're in the correct directory
cd "$(dirname "$0")/.."

# Build and run
docker build -t local-kafka-tester -f local_testing/Dockerfile .
# Run make test
# docker run --rm -it -v $(pwd):/app local-git-tester make test
# Generate fixtures
# docker run --rm -it -e CODECRAFTERS_RECORD_FIXTURES=true -v $(pwd):/app local-kafka-tester make test
# Run test_all/
docker run --rm -it -v $(pwd):/app local-kafka-tester make test