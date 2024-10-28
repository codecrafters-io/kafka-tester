package internal

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	tester_utils_testing "github.com/codecrafters-io/tester-utils/testing"
)

func TestStages(t *testing.T) {
	os.Setenv("CODECRAFTERS_RANDOM_SEED", "1234567890")

	testCases := map[string]tester_utils_testing.TesterOutputTestCase{
		"base_stages_pass": {
			UntilStageSlug:      "pv1",
			CodePath:            "./test_helpers/pass_all",
			ExpectedExitCode:    0,
			StdoutFixturePath:   "./test_helpers/fixtures/base/pass",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
		"concurrent_stages_pass": {
			StageSlugs:          []string{"nh4", "sk0"},
			CodePath:            "./test_helpers/pass_all",
			ExpectedExitCode:    0,
			StdoutFixturePath:   "./test_helpers/fixtures/concurrent_stages/pass",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
		"describe_topic_partitions_pass": {
			StageSlugs:          []string{"yk1", "vt6", "ea7", "ku4", "wq2"},
			CodePath:            "./test_helpers/pass_all",
			ExpectedExitCode:    0,
			StdoutFixturePath:   "./test_helpers/fixtures/describe_topic_partitions/pass",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
		"fetch_pass": {
			StageSlugs:          []string{"gs0", "dh6", "hn6", "cm4", "eg2", "fd8"},
			CodePath:            "./test_helpers/pass_all",
			ExpectedExitCode:    0,
			StdoutFixturePath:   "./test_helpers/fixtures/fetch/pass",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
	}

	tester_utils_testing.TestTesterOutput(t, testerDefinition, testCases)
}

func TestOutputNormalization(t *testing.T) {
	lines := strings.Trim(`
[33m[stage-6] [0m[36mIdx  | Hex                                             | ASCII[0m
[33m[stage-6] [0m[36m-----+-------------------------------------------------+-----------------[0m
[33m[stage-6] [0m[36m0000 | 0a 95 c1 bf 00 00 3e 00 00 00 00 00 0b 00 00 01 | ......>.........[0m
[33m[stage-6] [0m[36m0210 | 73 69 6f 6e 00 14 00 14 00                      | sion.....[0m
`, "\n")

	expectedOutput := strings.Trim(`
[33m[stage-6] [0m[36mIdx  | Hex                                             | ASCII[0m
[33m[stage-6] [0m[36m-----+-------------------------------------------------+-----------------[0m
hexdump
`, "\n")

	fmt.Println("---")
	fmt.Println(string(normalizeTesterOutput([]byte(lines))))
	fmt.Println("---")
	fmt.Println("---")
	fmt.Println(expectedOutput)
	fmt.Println("---")

	assert.Equal(t, normalizeTesterOutput([]byte(lines)), []byte(expectedOutput))
}

func normalizeTesterOutput(testerOutput []byte) []byte {
	replacements := map[string][]*regexp.Regexp{
		"hexdump":                {regexp.MustCompile(`(^\x1b\[33m\[stage-\d+\] \x1b\[0m\x1b\[36m\d{4} \| ([a-f0-9][a-f0-9] ){1,16} *\| [[:ascii:]]{1,16}\x1b\[0m\n?)+`)},
		"session_id":             {regexp.MustCompile(`- .session_id \([0-9]{0,16}\)`)},
		"leader_id":              {regexp.MustCompile(`- .leader_id \([-0-9]{1,}\)`)},
		"leader_epoch":           {regexp.MustCompile(`- .leader_epoch \([-0-9]{1,}\)`)},
		"wrote_file":             {regexp.MustCompile(`- Wrote file to: .*`)},
		"topic_id":               {regexp.MustCompile(`- .topic_id \([0-9]{8}-[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{12}\)`)},
		"compact_records_length": {regexp.MustCompile(`- .compact_records_length \([0-9]{2,}\)`)},
		"batch_length":           {regexp.MustCompile(`- .batch_length \([0-9]{2,}\)`)},
		"crc":                    {regexp.MustCompile(`- .crc \([-0-9]{1,}\)`)},
		"length":                 {regexp.MustCompile(`- .length \([0-9]{1,}\)`)},
		"Name":                   {regexp.MustCompile(`✓ Topic Name: [0-9A-Za-z]{3}`)},
		"UUID":                   {regexp.MustCompile(`✓ Topic UUID: [0-9]{8}-[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{12}`)},
		"value_length":           {regexp.MustCompile(`- .value_length \([0-9]{1,}\)`)},
		"value":                  {regexp.MustCompile(`- .[vV]alue \("[A-Za-z0-9 !]{1,}"\)`)},
		"name":                   {regexp.MustCompile(`- .name \([A-Za-z -]{1,}\)`)},
		"topic_name":             {regexp.MustCompile(`- .topic_name \([A-Za-z0-9 ]{1,}\)`)},
		"next_cursor":            {regexp.MustCompile(`- .next_cursor \(\{[A-Za-z0-9 ]{1,}\}\)`)},
		"Messages":               {regexp.MustCompile(`✓ Messages: \["[A-Za-z !]{1,}"\]`)},
		"Topic Name":             {regexp.MustCompile(`✓ TopicResponse\[[0-9]{1,}\] Topic Name: [A-Za-z -]{3,}`)},
		"Topic UUID":             {regexp.MustCompile(`✓ TopicResponse\[[0-9]{1,}\] Topic UUID: [0-9 -]{1,}`)},
		"Record Value":           {regexp.MustCompile(`✓ Record\[[0-9]{1,}\] Value: [A-Za-z0-9 !]{1,}`)},
		"RecordBatch BaseOffset": {regexp.MustCompile(`✓ RecordBatch\[[0-9]{1,}\] BaseOffset: [0-9]{1,}`)},
	}

	for replacement, regexes := range replacements {
		for _, regex := range regexes {
			testerOutput = regex.ReplaceAll(testerOutput, []byte(replacement))
		}
	}

	return testerOutput
}
