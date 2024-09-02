.PHONY: release build test test_with_bash copy_course_file

current_version_number := $(shell git tag --list "v*" | sort -V | tail -n 1 | cut -c 2-)
next_version_number := $(shell echo $$(($(current_version_number)+1)))

release:
	git tag v$(next_version_number)
	git push origin main v$(next_version_number)

build:
	go build -o dist/main.out ./cmd/tester

test_base_with_kafkax: build
	CODECRAFTERS_REPOSITORY_DIR=./internal/test_helpers/pass_all \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"st10\",\"tester_log_prefix\":\"stage-10\",\"title\":\"Stage #10: Fetch Error Response\"}]" \
	dist/main.out

test_base_with_kafka: build
	CODECRAFTERS_REPOSITORY_DIR=./internal/test_helpers/pass_all \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"st1\",\"tester_log_prefix\":\"stage-1\",\"title\":\"Stage #1: Bind to a port\"}, {\"slug\":\"st2\",\"tester_log_prefix\":\"stage-2\",\"title\":\"Stage #2: Hardcoded Correlation ID\"}, {\"slug\":\"st3\",\"tester_log_prefix\":\"stage-3\",\"title\":\"Stage #3: Correlation ID\"}, {\"slug\":\"st4\",\"tester_log_prefix\":\"stage-4\",\"title\":\"Stage #4: API Version Error Case\"}, {\"slug\":\"st6\",\"tester_log_prefix\":\"stage-6\",\"title\":\"Stage #6: API Version\"}, {\"slug\":\"st7\",\"tester_log_prefix\":\"stage-7\",\"title\":\"Stage #7: API Version with Fetch Key\"}, {\"slug\":\"st8\",\"tester_log_prefix\":\"stage-8\",\"title\":\"Stage #8: Fetch Error Response\"}, {\"slug\":\"st8_2\",\"tester_log_prefix\":\"stage-8\",\"title\":\"Stage #8: Fetch Error Response Lite Version\"}, {\"slug\":\"st9\",\"tester_log_prefix\":\"stage-9\",\"title\":\"Stage #9: Empty Fetch\"}, {\"slug\":\"st10\",\"tester_log_prefix\":\"stage-10\",\"title\":\"Stage #10: Fetch Error Response\"}]" \
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