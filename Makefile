.PHONY: release build test test_with_bash copy_course_file

current_version_number := $(shell git tag --list "v*" | sort -V | tail -n 1 | cut -c 2-)
next_version_number := $(shell echo $$(($(current_version_number)+1)))

release:
	git tag v$(next_version_number)
	git push origin main v$(next_version_number)

build:
	go build -o dist/main.out ./cmd/tester

test_base_with_kafka: build
	CODECRAFTERS_REPOSITORY_DIR=./internal/test_helpers/pass_all \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"vi6\",\"tester_log_prefix\":\"stage-1\",\"title\":\"Stage #1: Bind to a port\"}, {\"slug\":\"nv3\",\"tester_log_prefix\":\"stage-2\",\"title\":\"Stage #2: Hardcoded Correlation ID\"}, {\"slug\":\"wa6\",\"tester_log_prefix\":\"stage-3\",\"title\":\"Stage #3: Correlation ID\"}, {\"slug\":\"nc5\",\"tester_log_prefix\":\"stage-4\",\"title\":\"Stage #4: API Version Error Case\"}, {\"slug\":\"pv1\",\"tester_log_prefix\":\"stage-5\",\"title\":\"Stage #5: API Version\"}]" \
	dist/main.out

test_describe_topic_partitions_with_kafka: build
	CODECRAFTERS_REPOSITORY_DIR=./internal/test_helpers/pass_all \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"xy1\",\"tester_log_prefix\":\"stage-P1\",\"title\":\"Stage #P1: API Version with Describe Topic Partitions\"}, {\"slug\":\"xy2\",\"tester_log_prefix\":\"stage-P2\",\"title\":\"Stage #P2: Describe Topic Partitions for unknown topic\"}, {\"slug\":\"xy3\",\"tester_log_prefix\":\"stage-P3\",\"title\":\"Stage #P3: Describe Topic Partitions\"}, {\"slug\":\"xy4\",\"tester_log_prefix\":\"stage-P4\",\"title\":\"Stage #P4: Describe Topic Partitions w Multi partition\"}, {\"slug\":\"xy5\",\"tester_log_prefix\":\"stage-P5\",\"title\":\"Stage #P5: Describe Topic Partitions w Multi partition\"}, {\"slug\":\"xy6\",\"tester_log_prefix\":\"stage-P6\",\"title\":\"Stage #P6: Describe Topic Partitions 2\"}]" \
	dist/main.out

test_concurrent_requests_with_kafka: build
	CODECRAFTERS_REPOSITORY_DIR=./internal/test_helpers/pass_all \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"nh4\",\"tester_log_prefix\":\"stage-C1\",\"title\":\"Stage #C1: Multiple sequential requests from client\"}, {\"slug\":\"sk0\",\"tester_log_prefix\":\"stage-C2\",\"title\":\"Stage #C2: Multiple concurrent requests from client\"}]" \
	dist/main.out

test_fetch_partial_with_kafka: build
	CODECRAFTERS_REPOSITORY_DIR=./internal/test_helpers/pass_all \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"gs0\",\"tester_log_prefix\":\"stage-F1\",\"title\":\"Stage #F1: API Version with Fetch Key\"}, {\"slug\":\"dh6\",\"tester_log_prefix\":\"stage-F2\",\"title\":\"Stage #F2: Empty Fetch\"}, {\"slug\":\"hn6\",\"tester_log_prefix\":\"stage-F3\",\"title\":\"Stage #F3: Fetch with Empty Topic\"}, {\"slug\":\"cm4\",\"tester_log_prefix\":\"stage-F4\",\"title\":\"Stage #F4: Fetch Error Response\"}]" \
	dist/main.out

test_current:
	make test_base_with_kafka
	make test_concurrent_requests_with_kafka
	make test_describe_topic_partitions_with_kafka
	make test_fetch_partial_with_kafka

test_fetch_with_kafka: build
	CODECRAFTERS_REPOSITORY_DIR=./internal/test_helpers/pass_all \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"gs0\",\"tester_log_prefix\":\"stage-F1\",\"title\":\"Stage #F1: API Version with Fetch Key\"}, {\"slug\":\"dh6\",\"tester_log_prefix\":\"stage-F2\",\"title\":\"Stage #F2: Empty Fetch\"}, {\"slug\":\"hn6\",\"tester_log_prefix\":\"stage-F3\",\"title\":\"Stage #F3: Fetch with Empty Topic\"}, {\"slug\":\"cm4\",\"tester_log_prefix\":\"stage-F4\",\"title\":\"Stage #F4: Fetch Error Response\"}, {\"slug\":\"eg2\",\"tester_log_prefix\":\"stage-F5\",\"title\":\"Stage #F5: Single Fetch from Disk\"}, {\"slug\":\"fd8\",\"tester_log_prefix\":\"stage-F6\",\"title\":\"Stage #F6: Multi Fetch from Disk\"}]" \
	dist/main.out

test_75_with_kafka: build
	CODECRAFTERS_REPOSITORY_DIR=./internal/test_helpers/pass_all \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"xy6\",\"tester_log_prefix\":\"stage-P6\",\"title\":\"Stage #P6: Describe Topic Partitions 2\"}]" \
	dist/main.out

test:
	TESTER_DIR=$(shell pwd) go test -v ./internal/

test_and_watch:
	onchange '**/*' -- go test -v ./internal/

copy_course_file:
	hub api \
		repos/codecrafters-io/build-your-own-grep/contents/course-definition.yml \
		| jq -r .content \
		| base64 -d \
		> internal/test_helpers/course_definition.yml

update_tester_utils:
	go get -u github.com/codecrafters-io/tester-utils